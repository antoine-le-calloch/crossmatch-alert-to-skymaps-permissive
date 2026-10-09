import os
import time
import argparse
import traceback

from dotenv import load_dotenv

from utils.api import SkyPortal, APIError
from utils.logger import log, RED, ENDC, YELLOW
from utils.skymap import get_skymap, get_alias
from utils.kafka import read_avro, boom_consumer
from utils.converter import fallback, str_to_bool
from utils.gcn import prepare_gcn_payload
from utils.distance import get_distance_info, is_distance_consistent, format_distance_info
from utils.gw_alerts import get_gw_alert_listener
from utils.published_matches import load_published_matches, save_published_matches

load_dotenv()

SKYPORTAL_URL = os.getenv("SKYPORTAL_URL")
SKYPORTAL_API_KEY = os.getenv("SKYPORTAL_API_KEY")
BOOM_FILTERS = os.getenv("BOOM_KAFKA_FILTERS").split(",")
NOTIFY_GCN = str_to_bool(os.getenv("NOTIFY_GCN"), default=False)
NOTIFY_SLACK = str_to_bool(os.getenv("NOTIFY_SLACK"), default=False)

GCN = 24*8  # hours for GCN fallback
FIRST_DETECTION = 24*7  # hours for first detection fallback
SLEEP_TIME = 20 # seconds between each loop
HEARTBEAT_INTERVAL = 120 # seconds between each heartbeat log
MAX_GW_AREA_90 = 5000 # sq. deg., larger GW skymaps are ignored
NS_PROBABILITY_THRESHOLD = 0.1 # minimum P(BNS) + P(NSBH) of a GW event


def get_all_photometry(alert):
    """Photometry of the alert and of its crossmatched objects in the other surveys, sorted by jd."""
    photometry = list(alert.get("photometry") or [])
    for survey_match in (alert.get("survey_matches") or {}).values():
        if survey_match:
            photometry += survey_match.get("photometry") or []
    return sorted(photometry, key=lambda phot: phot["jd"])


def get_filtered_photometry(alert, snr_threshold, first_detection_fallback):
    """
    Filter the photometry of an alert (and of its crossmatches in the other surveys) to keep only
    the last non-detection, if any, and all detections, while also checking if the object is too
    old based on the first detection fallback.

    A detection is any photometry point (including forced photometry) with SNR >= snr_threshold.
    Points with missing flux_err and alert points with negative flux are skipped entirely; points
    below the SNR threshold (including negative forced photometry) are treated as non-detection.

    Parameters
    ----------
    alert : dict
        The alert containing photometry data.
    snr_threshold : float
        The SNR threshold above which a photometry point is considered a detection.
    first_detection_fallback : float
        The Julian Date fallback for the first detection
    Returns
    -------
    list or None
        A list of photometry points that includes the last non-detection (flux None), if any,
        and all detections, or None if too old or if there are no detections.
    """
    last_non_detection = []
    filtered_photometry = []
    for phot in reversed(get_all_photometry(alert)):  # From the most recent to the oldest
        is_forced = phot["origin"] == "ForcedPhot"
        if not phot["flux_err"] or (not is_forced and phot["flux"] and phot["flux"] < 0):
            continue # Skip no flux_err and negative alert fluxes

        snr = phot["flux"] / phot["flux_err"] if phot["flux"] else 0
        if snr >= snr_threshold:  # Detection
            if phot["jd"] < first_detection_fallback:
                if is_forced and snr < 5:
                    continue # A low SNR forced photometry point is not enough to call the object too old
                # A detection older than first_detection_fallback means the object is too old, skip it
                return None
            last_non_detection = []  # Reset last non-detection as we found a detection
            filtered_photometry.append(phot)
        elif not last_non_detection:
            last_non_detection = [{**phot, "flux": None}]

    if not filtered_photometry:
        log(f"{RED}Alert {alert['objectId']} does not have any valid detection.{ENDC}")
        return None

    # Keep the last non-detection and all detections
    return last_non_detection + list(reversed(filtered_photometry))


def get_first_detection_jd(filtered_photometry):
    return next(phot["jd"] for phot in filtered_photometry if phot["flux"] is not None)


def crossmatch_and_publish(alert, filtered_photometry, candidate_skymaps, published_matches, gcn=None, slack=None):
    """Crossmatch an alert with candidate skymaps and publish a notice for the new matches."""
    obj_id = alert["objectId"]
    first_detection_jd = get_first_detection_jd(filtered_photometry)
    last_non_detection_jd = filtered_photometry[0]["jd"] if filtered_photometry[0]["flux"] is None else None
    matching_skymaps = {}
    notes = []
    for dateobs, skymap in candidate_skymaps.items():
        if skymap.jd > first_detection_jd or (last_non_detection_jd is not None and skymap.jd < last_non_detection_jd):
            continue # Skymap is not between the last non-detection (if any) and the first detection

        if obj_id in published_matches and (dateobs, skymap.created_at) in published_matches[obj_id].get("skymaps", set()):
            log(f"Skipping already processed skymap {dateobs} for object {obj_id}")
            continue # This skymap has already been processed for this object

        if skymap.type == "GW" and skymap.area_90 and skymap.area_90 > MAX_GW_AREA_90:
            continue # Too large to be informative, most new transients in it would match

        if skymap.contains(alert["ra"], alert["dec"]):
            matching_skymaps[dateobs] = skymap
            distance_info = get_distance_info(skymap, alert, filtered_photometry)
            if distance_info:
                consistency = "" if is_distance_consistent(distance_info) else " (INCONSISTENT)"
                notes.append(f"{skymap.alias}: {format_distance_info(distance_info)}{consistency}")

    if not matching_skymaps:
        return

    skymaps_string = ", ".join(skymap.name for skymap in matching_skymaps.values())
    log(f"{obj_id} matches the following skymaps: {skymaps_string}")
    alert["filtered_photometry"] = filtered_photometry
    gcn_payload = prepare_gcn_payload(alert, matching_skymaps)

    if gcn:
        gcn.produce(gcn_payload)
    if slack:
        slack.send(alert, matching_skymaps, gcn_payload, notes)

    # Add the object and matching skymaps to published_matches to avoid re-processing
    dateobs_created_at_tuple = set((dateobs, skymap.created_at) for dateobs, skymap in matching_skymaps.items())
    if obj_id not in published_matches:
        published_matches[obj_id] = {
            "skymaps": dateobs_created_at_tuple,
            "first_detection_jd": first_detection_jd,
        }
    else:
        published_matches[obj_id]["skymaps"].update(dateobs_created_at_tuple)
    save_published_matches(published_matches)


def boom_gcn_pipeline(gcn=None, slack=None):
    skyportal = SkyPortal(instance=SKYPORTAL_URL, token=SKYPORTAL_API_KEY)
    cumulative_probability = 0.95
    snr_threshold = 3.0
    published_matches = load_published_matches()  # {objectId: {"skymaps": set((dateobs,created_at)), "first_detection_jd": float}}
    if published_matches:
        log(f"Loaded {len(published_matches)} objects already published by the previous run")
    skymaps = {} # {dateobs: Skymap}
    skipped_events = set() # {dateobs} of events already reported as not usable
    recent_alerts = {}  # {objectId: (alert, filtered_photometry)} to crossmatch with skymaps received later

    check_for_gcn_events_timer = None
    heartbeat_timer = time.time()
    total_processed_alerts = 0
    new_processed_alerts = 0
    log_empty_poll = True

    consumer = boom_consumer()
    log(f"Listening for alerts passing the following Boom filters: {BOOM_FILTERS}")
    gw_alerts = get_gw_alert_listener(GCN)

    while True:
        if time.time() - heartbeat_timer >= HEARTBEAT_INTERVAL:
            heartbeat_timer = time.time()
            if gcn:
                gcn.heartbeat()

        try:
            # only check that every SLEEP_TIME seconds to avoid hitting the API
            if not check_for_gcn_events_timer or time.time() - check_for_gcn_events_timer >= SLEEP_TIME:
                check_for_gcn_events_timer = time.time() # reset timer

                gcn_events = []
                if gw_alerts:
                    gcn_fallback_dateobs = fallback(GCN, date_format="iso")[:19]
                    gw_alerts.poll(gcn_fallback_dateobs)
                    gcn_events += gw_alerts.get_events(gcn_fallback_dateobs, NS_PROBABILITY_THRESHOLD)

                # Check if SkyPortal is available
                skyportal_available = skyportal.ping()
                if skyportal_available:
                    gcn_events += skyportal.get_gcn_events(fallback(GCN), NS_PROBABILITY_THRESHOLD, include_gw=gw_alerts is None)
                else:
                    log(f"{YELLOW}SkyPortal API is not available, keeping the skymaps already fetched{ENDC}")

                if skyportal_available or gw_alerts:
                    # Check for new GCN events or new localizations for existing events (GW of any size, others with "< 1000 sq. deg." tag)
                    new_gcn_events = []
                    for event in gcn_events:
                        if not get_alias(event):
                            if event["dateobs"] not in skipped_events:
                                skipped_events.add(event["dateobs"])
                                log(f"Skipping GCN event {event['dateobs']} due to bad aliases: {event.get('aliases')}")
                            continue # Filter out GCN events with bad or no aliases

                        is_gw = "GW" in (event.get("tags") or [])
                        event["localization"] = next(
                            (loc for loc in event.get("localizations", [])
                             if is_gw or any(tag["text"] == "< 1000 sq. deg." for tag in loc.get("tags", []))),
                            None
                        )
                        if event["localization"] is None:
                            continue
                        elif event["dateobs"] not in skymaps:
                            new_gcn_events.append(event)
                        elif skymaps[event["dateobs"]].created_at < event["localization"]["created_at"]:
                            # If the localization is newer than the one we have for that dateobs, we should recompute this event
                            new_gcn_events.append(event)

                    new_skymaps = {}
                    for gcn_event in new_gcn_events:
                        try:
                            skymap = get_skymap(skyportal, cumulative_probability, gcn_event)
                        except Exception as e:
                            log(f"{YELLOW}Could not fetch the skymap of {get_alias(gcn_event)}/{gcn_event['dateobs']}, retrying at the next check: {e}{ENDC}")
                            continue
                        skymaps[gcn_event["dateobs"]] = skymap
                        new_skymaps[gcn_event["dateobs"]] = skymap
                        log(f"Fetched skymap {skymap.name} and created its MOC (90% area: {f'{skymap.area_90:.0f}' if skymap.area_90 >= 100 else f'{skymap.area_90:.3g}'} sq. deg.)")

                    # Clean up old skymaps (GCN events older than fallback)
                    gcn_fallback_iso = fallback(GCN, date_format="iso")[:19]
                    skipped_events = {d for d in skipped_events if d >= gcn_fallback_iso}
                    for dateobs in list(skymaps.keys()):
                        if dateobs < gcn_fallback_iso:
                            log(f"Removed expired skymap {dateobs} from skymaps")
                            del skymaps[dateobs]

                    first_detection_fallback_jd = fallback(FIRST_DETECTION, date_format="jd")
                    expired_objects = [obj_id for obj_id, info in published_matches.items() if info["first_detection_jd"] < first_detection_fallback_jd]
                    for obj_id in expired_objects:
                        log(f"Removed expired object {obj_id} from published_matches")
                        del published_matches[obj_id]
                    if expired_objects:
                        save_published_matches(published_matches)
                    for obj_id, (_, filtered_photometry) in list(recent_alerts.items()):
                        if get_first_detection_jd(filtered_photometry) < first_detection_fallback_jd:
                            del recent_alerts[obj_id]

                    new_skymaps = {dateobs: skymap for dateobs, skymap in new_skymaps.items() if dateobs in skymaps}
                    if new_skymaps and recent_alerts:
                        log(f"Crossmatching {len(recent_alerts)} recent alerts with {len(new_skymaps)} new skymaps")
                        for alert, filtered_photometry in list(recent_alerts.values()):
                            crossmatch_and_publish(alert, filtered_photometry, new_skymaps, published_matches, gcn, slack)

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

            alert = read_avro(msg)
            if not any(boom_filter.get("filter_name") in BOOM_FILTERS for boom_filter in alert.get("filters", [])):
                continue
            log_empty_poll = True
            new_processed_alerts += 1

            filtered_photometry = get_filtered_photometry(alert, snr_threshold, fallback(FIRST_DETECTION, date_format="jd"))
            if not filtered_photometry:
                continue # The First detection is too old or the alert doesn't have any detections

            for cutout in ("cutoutScience", "cutoutTemplate", "cutoutDifference"):
                alert.pop(cutout, None)
            recent_alerts[alert["objectId"]] = (alert, filtered_photometry)

            if skymaps:
                crossmatch_and_publish(alert, filtered_photometry, skymaps, published_matches, gcn, slack)

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
    log(f"{YELLOW}GCN notifier is disabled in this repo: private ZTF alerts must not be republished to public GCN streams.{ENDC}")
    # if NOTIFY_GCN:
    #     from gcn.produce_gcn_notices import GcnProducer
    #     gcn_notifier = GcnProducer()

    if not gcn_notifier and not slack_notifier:
        raise SystemExit(f"{RED}No notifier enabled. Please enable at least one of GCN or Slack notifications.{ENDC}")

    boom_gcn_pipeline(gcn=gcn_notifier, slack=slack_notifier)
