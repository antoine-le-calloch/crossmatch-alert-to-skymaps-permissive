import io
import os
import json
import time
import base64

from datetime import datetime, timedelta, UTC
from gcn_kafka import Consumer

from utils.logger import log
from utils.skymap import read_skymap_probabilities, get_area_90

GW_ALERT_TOPIC = "igwn.gwalert"
LOCALIZATION_AREA_TAGS = (200, 500, 1000)  # sq. deg., the "< N sq. deg." tags SkyPortal puts on localizations
MAX_POLL_DURATION = 10  # seconds spent reading GW alerts per poll, so that BOOM alerts keep being processed


def get_dateobs(event_time):
    """Event time rounded to the nearest second, formatted like a SkyPortal dateobs."""
    time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    return (time + timedelta(milliseconds=500)).strftime("%Y-%m-%dT%H:%M:%S")


def get_gw_alert_listener(lookback_hours):
    """
    Listen to GW alerts directly from GCN if GCN consumer credentials are set.

    Parameters
    ----------
    lookback_hours : float
        How far back to read the GW alerts at startup

    Returns
    -------
    GwAlertListener or None
        None when GCN_KAFKA_CONSUMER_CLIENT_ID or GCN_KAFKA_CONSUMER_CLIENT_SECRET is missing,
        in which case GW events are fetched from SkyPortal.
    """
    client_id = os.getenv("GCN_KAFKA_CONSUMER_CLIENT_ID")
    client_secret = os.getenv("GCN_KAFKA_CONSUMER_CLIENT_SECRET")
    if not client_id or not client_secret:
        log("No GCN consumer credentials, GW events are fetched from SkyPortal")
        return None
    return GwAlertListener(client_id, client_secret, lookback_hours)


class GwAlertListener:
    """
    GW superevents received on the GCN igwn.gwalert topic, selected like the GW events that SkyPortal ingests.

    Alerts from mock data challenges (search MDC) are rejected, the dateobs is the time of the first notice
    rounded to the nearest second, and each localization gets the same "< N sq. deg." tags as in SkyPortal.
    Skymaps are only decoded for the events that pass the neutron star probability threshold, and only the
    newest localization and the newest one under 1000 sq. deg. are then kept: the pipelines never use the others.
    """

    def __init__(self, client_id, client_secret, lookback_hours):
        self.consumer = Consumer(
            client_id=client_id,
            client_secret=client_secret,
            domain="gcn.nasa.gov",
            config={
                "group.id": f"boom_gcn_pipeline_gw_alerts_{datetime.now(UTC).strftime('%Y_%m_%d_%H_%M_%S')}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            },
        )
        start_timestamp_ms = int((time.time() - lookback_hours * 3600) * 1000)

        def start_at_lookback(consumer, partitions):
            for partition in partitions:
                partition.offset = start_timestamp_ms
            consumer.assign(consumer.offsets_for_times(partitions, timeout=30))

        self.consumer.subscribe([GW_ALERT_TOPIC], on_assign=start_at_lookback)
        self.superevents = {}  # {superevent_id: {"dateobs", "retracted", "pipelines", "classification", "localizations"}}
        log(f"Listening for GW alerts on the GCN topic {GW_ALERT_TOPIC}")

    def poll(self, start_dateobs):
        """Process the GW alerts received since the last poll, for at most MAX_POLL_DURATION seconds, ignoring superevents older than start_dateobs."""
        deadline = time.time() + MAX_POLL_DURATION
        while time.time() < deadline and (messages := self.consumer.consume(num_messages=50, timeout=1)):
            for message in messages:
                if message.error():
                    log(f"GCN consumer error: {message.error()}")
                    continue
                self.process_alert(json.loads(message.value()), start_dateobs)

    def process_alert(self, alert, start_dateobs):
        """Update the superevent of a GW alert with its classification, pipeline, localization or retraction."""
        superevent_id = alert.get("superevent_id")
        event = alert.get("event") or {}
        if not superevent_id or event.get("search") == "MDC":
            return
        if alert.get("alert_type") == "RETRACTION":
            if superevent_id in self.superevents:
                self.superevents[superevent_id]["retracted"] = True
            return
        if not event.get("time"):
            return
        if superevent_id not in self.superevents and get_dateobs(event["time"]) < start_dateobs:
            return

        superevent = self.superevents.setdefault(superevent_id, {
            "dateobs": get_dateobs(event["time"]),
            "retracted": False,
            "pipelines": set(),
            "classification": {},
            "localizations": [],
        })
        superevent["pipelines"].add(event.get("pipeline"))
        if event.get("classification"):
            superevent["classification"] = event["classification"]
        if event.get("skymap"):
            superevent["localizations"].insert(0, {
                "dateobs": superevent["dateobs"],
                "localization_name": event.get("skymap_filename"),
                "created_at": alert["time_created"].replace("Z", ""),
                "skymap": event["skymap"],
            })

    @staticmethod
    def decode_localizations(localizations):
        """Decode the skymaps and tag them with their size, keeping the newest one and the newest one under 1000 sq. deg."""
        for localization in localizations:
            if "fits" not in localization:
                localization["fits"] = base64.b64decode(localization.pop("skymap"))
                area_90 = get_area_90(*read_skymap_probabilities(io.BytesIO(localization["fits"]))[:2])
                localization["tags"] = [{"text": f"< {area} sq. deg."} for area in LOCALIZATION_AREA_TAGS if area_90 < area]
        newest_small = next(
            (loc for loc in localizations if any(tag["text"] == "< 1000 sq. deg." for tag in loc["tags"])),
            None
        )
        return localizations[:1] + ([newest_small] if newest_small is not None and newest_small is not localizations[0] else [])

    def get_events(self, start_dateobs, ns_probability_threshold):
        """
        GW events with a dateobs after start_dateobs, as returned by SkyPortal.get_gcn_events

        Parameters
        ----------
        start_dateobs : str
            Oldest dateobs to keep, as "YYYY-MM-DDTHH:MM:SS"
        ns_probability_threshold : float
            Minimum P(BNS) + P(NSBH) of the most recent notice for a GW event to be kept

        Returns
        -------
        list
            GCN events not retracted, never seen by the MLy pipeline, and likely to contain a neutron star
        """
        self.superevents = {
            superevent_id: superevent for superevent_id, superevent in self.superevents.items()
            if superevent["dateobs"] >= start_dateobs
        }
        events = []
        for superevent_id, superevent in self.superevents.items():
            classification = superevent["classification"]
            if (
                superevent["retracted"]
                or "MLy" in superevent["pipelines"]
                or (classification.get("BNS") or 0) + (classification.get("NSBH") or 0) < ns_probability_threshold
            ):
                continue
            if superevent["localizations"]:
                superevent["localizations"] = self.decode_localizations(superevent["localizations"])
            events.append({
                "dateobs": superevent["dateobs"],
                "aliases": [f"LVC#{superevent_id}"],
                "tags": ["GW"],
                "localizations": superevent["localizations"],
            })
        return events
