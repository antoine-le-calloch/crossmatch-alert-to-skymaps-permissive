import os
import time
import argparse
import traceback

from dotenv import load_dotenv

from utils.api import SkyPortal, APIError
from utils.logger import log, RED, ENDC, YELLOW
from utils.skymap import get_skymap
from utils.kafka import read_avro, boom_consumer
from utils.converter import fallback, str_to_bool
from utils.gcn import prepare_gcn_payload

load_dotenv()

SKYPORTAL_URL = os.getenv("SKYPORTAL_URL")
SKYPORTAL_API_KEY = os.getenv("SKYPORTAL_API_KEY")
BOOM_FILTERS = os.getenv("BOOM_KAFKA_FILTERS").split(",")
NOTIFY_GCN = str_to_bool(os.getenv("NOTIFY_GCN"), default=False)
NOTIFY_SLACK = str_to_bool(os.getenv("NOTIFY_SLACK"), default=False)

GCN = 24*6  # hours for GCN fallback
FIRST_DETECTION = 24*5  # hours for first detection fallback
SLEEP_TIME = 20 # seconds between each loop
HEARTBEAT_INTERVAL = 120 # seconds between each heartbeat log


def get_filtered_photometry(alert, snr_threshold, first_detection_fallback):
    """
    Filter the photometry of an alert to keep only the last non-detection and all detections,
    while also checking if the object is too old based on the SNR threshold and the first detection fallback.

    Parameters
    ----------
    alert : dict
        The alert containing photometry data.
    snr_threshold : float
        The SNR threshold to consider an object as too old.
    first_detection_fallback : float
        The Julian Date fallback for the first detection
    Returns
    -------
    list or None
        A list of photometry points that includes the last non-detection and all detections,
        or None if too old or if there are no non-detections.
    """
    last_non_detection = []
    filtered_photometry = []
    for phot in reversed(alert.get("photometry", [])):  # From the most recent to the oldest
        if phot["programid"] != 1:
            continue
        if phot["origin"] == "ForcedPhot" or not phot["flux_err"] or (phot["flux"] and phot["flux"] < 0):
            continue # Skip forced photometry, no flux_err and negative fluxes

        if phot["flux"]:  # If it's a detection
            last_non_detection = []  # Reset last non-detection as we found a detection
            filtered_photometry.append(phot)
            if phot["flux"] / phot["flux_err"] >= snr_threshold and phot["jd"] < first_detection_fallback:
                # If at least one detection with SNR >= snr_threshold is older than first_detection_fallback, consider the object as too old and skip it
                return None
        elif not last_non_detection:
            last_non_detection = [phot]

    if not filtered_photometry and not last_non_detection:
        log(f"{RED}Alert {alert['objectId']} does not have any valid detection or non-detection.{ENDC}")
        return None
    if not last_non_detection:
        log(f"{YELLOW}Alert {alert['objectId']} does not have any non-detection before the first detection, skipping it.{ENDC}")
        return None

    # Keep the last non-detection and all detections
    return last_non_detection + list(reversed(filtered_photometry))


def boom_gcn_pipeline(gcn=None, slack=None):
    skyportal = SkyPortal(instance=SKYPORTAL_URL, token=SKYPORTAL_API_KEY)
    cumulative_probability = 0.95
    snr_threshold = 5.0
    published_matches = {}  # {objectId: {"skymaps": set((dateobs,created_at)), "first_detection_jd": float}}
    skymaps = {} # {dateobs: Skymap}

    check_for_gcn_events_timer = None
    heartbeat_timer = time.time()
    total_processed_alerts = 0
    new_processed_alerts = 0
    log_empty_poll = True

    consumer = boom_consumer()
    log(f"Listening for alerts passing the following Boom filters: {BOOM_FILTERS}")

    while True:
        if time.time() - heartbeat_timer >= HEARTBEAT_INTERVAL:
            heartbeat_timer = time.time()
            if gcn:
                gcn.heartbeat()

        try:
            # only check that every SLEEP_TIME seconds to avoid hitting the API
            if not check_for_gcn_events_timer or time.time() - check_for_gcn_events_timer >= SLEEP_TIME:
                check_for_gcn_events_timer = time.time() # reset timer

                # Check if SkyPortal is available
                skyportal.ping()

                # Check for new GCN events or new localizations for existing events with "< 1000 sq. deg." tag
                new_gcn_events = []
                for event in skyportal.get_gcn_events(fallback(GCN)):
                    if not event.get("aliases") or not any("#" in alias for alias in event["aliases"]):
                        if event.get("aliases"):
                            log(f"Skipping GCN event {event['dateobs']} due to bad aliases: {event['aliases']}")
                        continue # Filter out GCN events with bad or no aliases

                    event["localization"] = next(
                        (loc for loc in event.get("localizations", [])
                         if any(tag["text"] == "< 1000 sq. deg." for tag in loc.get("tags", []))),
                        None
                    )
                    if event["localization"] is None:
                        continue
                    elif event["dateobs"] not in skymaps:
                        new_gcn_events.append(event)
                    elif skymaps[event["dateobs"]].created_at < event["localization"]["created_at"]:
                        # If the localization is newer than the one we have for that dateobs, we should recompute this event
                        new_gcn_events.append(event)

                for gcn_event in new_gcn_events:
                    skymaps[gcn_event["dateobs"]] = get_skymap(skyportal, cumulative_probability, gcn_event)
                if new_gcn_events:
                    log(f"Fetched {len(new_gcn_events)} skymaps and created MOCs")

                # Clean up old skymaps (GCN events older than fallback)
                gcn_fallback_iso = fallback(GCN, date_format="iso")[:19]
                for dateobs in list(skymaps.keys()):
                    if dateobs < gcn_fallback_iso:
                        log(f"Removed expired skymap {dateobs} from skymaps")
                        del skymaps[dateobs]

                first_detection_fallback_jd = fallback(FIRST_DETECTION, date_format="jd")
                for obj_id, info in list(published_matches.items()):
                    if info["first_detection_jd"] < first_detection_fallback_jd:
                        log(f"Removed expired object {obj_id} from published_matches")
                        del published_matches[obj_id]

            # Consume new alerts passing a set of filters from Boom Kafka and crossmatch them with available skymaps
            msg = consumer.poll(timeout=30.0)
            if msg is None:
                if new_processed_alerts:
                    total_processed_alerts += new_processed_alerts
                    log(f"{new_processed_alerts} new alerts processed ({total_processed_alerts} total)")
                    new_processed_alerts = 0
                if log_empty_poll:
                    log(f"No new alerts from Boom Kafka, waiting...")
                    log_empty_poll = False
                continue
            if msg.error():
                log(f"Consumer error: {msg.error()}")
                continue

            if skymaps:
                alert = read_avro(msg)

                if not any(boom_filter.get("filter_name") in BOOM_FILTERS for boom_filter in alert.get("filters", [])):
                    continue
                log_empty_poll = True
                obj_id = alert["objectId"]
                new_processed_alerts += 1

                filtered_photometry = get_filtered_photometry(alert, snr_threshold, fallback(FIRST_DETECTION, date_format="jd"))
                if not filtered_photometry or len(filtered_photometry) < 2:
                    continue # The First detection is too old or the alert doesn't have any detections/non-detections

                matching_skymaps = {}
                for dateobs, skymap in skymaps.items():
                    if not filtered_photometry[0]["jd"] <= skymap.jd <= filtered_photometry[1]["jd"]:
                        continue # Skymap is not between the last non-detection and the first detection

                    if obj_id in published_matches and (dateobs, skymap.created_at) in published_matches[obj_id].get("skymaps", set()):
                        log(f"Skipping already processed skymap {dateobs} for object {obj_id}")
                        continue # This skymap has already been processed for this object

                    if skymap.contains(alert["ra"], alert["dec"]):
                        # If the object is in the skymap, add it to the matching_skymaps dictionary
                        matching_skymaps[dateobs] = skymap

                if matching_skymaps:
                    # Process the crossmatch results here (e.g., send to GCN, log, etc.)
                    skymaps_string = ", ".join(skymap.name for skymap in matching_skymaps.values())
                    log(f"{obj_id} matches the following skymaps: {skymaps_string}")
                    alert["filtered_photometry"] = filtered_photometry
                    gcn_payload = prepare_gcn_payload(alert, matching_skymaps)

                    # Publish the GCN notice with the alert data and matching skymaps to the GCN Kafka topic
                    if gcn:
                        gcn.produce(gcn_payload)

                    # Send the GCN notice with the alert data and matching skymaps to Slack
                    if slack:
                        slack.send(alert, matching_skymaps, gcn_payload)

                    # Add the object and matching skymaps to published_matches to avoid re-processing
                    dateobs_created_at_tuple = set((dateobs, skymap.created_at) for dateobs, skymap in matching_skymaps.items())
                    if obj_id not in published_matches:
                        published_matches[obj_id] = {
                            "skymaps": dateobs_created_at_tuple,
                            "first_detection_jd": filtered_photometry[1]["jd"],
                        }
                    else:
                        published_matches[obj_id]["skymaps"].update(dateobs_created_at_tuple)

        except APIError as e:
            log(e)
        except Exception:
            log(f"{RED}An error occurred:{ENDC}")
            traceback.print_exc()


if __name__ == "__main__":
    # --- CLI arguments ---
    parser = argparse.ArgumentParser(
        description="Crossmatch alerts with GCN skymaps.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--gcn",
        "-g",
        type=int,
        default=GCN,
        help="GCN fallback in hours.",
    )
    parser.add_argument(
        "--detection",
        "-d",
        type=int,
        default=FIRST_DETECTION,
        help="First detection fallback in hours.",
    )
    parser.add_argument(
        "--sleep-time",
        "-s",
        type=int,
        default=SLEEP_TIME,
        help="Time in seconds to wait between each check for new GCN events.",
    )
    parser.add_argument(
        "--clean-slack",
        "-cs",
        action="store_true",
        help="Whether to delete all current bot messages in the Slack channel before starting the script.",
    )
    args = parser.parse_args()
    GCN = args.gcn
    FIRST_DETECTION = args.detection
    SLEEP_TIME = args.sleep_time

    slack_notifier = None
    if NOTIFY_SLACK:
        from utils.slack import SlackNotifier
        slack_notifier = SlackNotifier()
        if args.clean_slack:
            slack_notifier.delete_all_bot_messages()

    gcn_notifier = None
    if NOTIFY_GCN:
        from gcn.produce_gcn_notices import GcnProducer
        gcn_notifier = GcnProducer()

    if not gcn_notifier and not slack_notifier:
        raise SystemExit(f"{RED}No notifier enabled. Please enable at least one of GCN or Slack notifications.{ENDC}")

    boom_gcn_pipeline(gcn=gcn_notifier, slack=slack_notifier)
