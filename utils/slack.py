import io
import json
import os
import time
import urllib.error
from collections import Counter

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from utils.converter import flux_to_mag, get_survey
from utils.logger import log, RED, YELLOW, ENDC
from utils.skymap import plot_object_on_skymap

load_dotenv()

UPLOAD_ATTEMPTS = 3
UPLOAD_RETRY_DELAY = 10  # seconds, doubled after each failed attempt
SURVEY_NAMES = {"LSSTCam": "LSST"}


def is_transient(error):
    """Whether a Slack upload error is worth retrying: server errors, rate limits, timeouts and connection errors."""
    if isinstance(error, urllib.error.HTTPError):
        return error.code >= 500 or error.code == 429
    if isinstance(error, SlackApiError):
        return error.response.status_code >= 500 or error.response.status_code == 429
    return isinstance(error, (urllib.error.URLError, TimeoutError, ConnectionError))


class SlackNotifier:
    def __init__(self):
        bot_token = os.getenv("SLACK_BOT_TOKEN")
        channel_name = os.getenv("SLACK_CHANNEL_NAME")
        if not bot_token or not channel_name:
            raise RuntimeError("SLACK_BOT_TOKEN and SLACK_CHANNEL_NAME must be set.")
        self.skyportal_url = os.getenv("SKYPORTAL_URL")
        self.client = WebClient(token=bot_token)
        self.channel_id = self._get_channel_id(channel_name)
        if not self.channel_id:
            raise RuntimeError(f"Slack channel '{channel_name}' not found.")

    def _get_channel_id(self, channel_name):
        """Get the Slack channel ID for the specified channel name."""
        cursor = None
        while True:
            response = self.client.conversations_list(
                types="public_channel,private_channel",
                limit=200,
                cursor=cursor,
            )
            for channel in response.get("channels", []):
                if channel["name"] == channel_name:
                    return channel["id"]
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                return None

    def delete_all_bot_messages(self):
        """Delete all messages sent by the bot in the specified Slack channel."""
        history = self.client.conversations_history(channel=self.channel_id)
        for message in history["messages"]:
            if message.get("bot_id"):
                self.client.chat_delete(channel=self.channel_id, ts=message["ts"])
                time.sleep(1.3)  # To avoid hitting rate limits

    def _upload(self, files, **kwargs):
        """files_upload_v2 of [(filename, bytes)] in the channel, retried on transient errors."""
        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            uploads = [{"file": io.BytesIO(content), "filename": name, "title": name} for name, content in files]
            try:
                return self.client.files_upload_v2(channel=self.channel_id, file_uploads=uploads, **kwargs)
            except Exception as e:
                if attempt == UPLOAD_ATTEMPTS or not is_transient(e):
                    raise
                delay = UPLOAD_RETRY_DELAY * 2 ** (attempt - 1)
                log(f"{YELLOW}Slack upload failed ({e}), retrying in {delay} seconds{ENDC}")
                time.sleep(delay)

    def _share_ts(self, response):
        """Timestamp of the message that shares an upload in the channel, or None if Slack does not give it in time."""
        file_id = response["files"][0]["id"]
        for _ in range(10):
            time.sleep(2)
            shares = self.client.files_info(file=file_id)["file"].get("shares") or {}
            for kind in ("public", "private"):
                for share in shares.get(kind, {}).get(self.channel_id, []):
                    return share["ts"]
        return None

    def post(self, text):
        """Post a text message in the channel, return its timestamp."""
        return self.client.chat_postMessage(channel=self.channel_id, text=text, unfurl_links=False)["ts"]

    def reply(self, thread_ts, text):
        """Post a text message in the thread of a message."""
        self.client.chat_postMessage(channel=self.channel_id, thread_ts=thread_ts, text=text, unfurl_links=False)

    def update(self, ts, text):
        """Replace the text of a message, keeping its images."""
        self.client.chat_update(channel=self.channel_id, ts=ts, text=text)

    def header(self, obj, matching_skymaps):
        """Object, matching events and a one line summary of the detections."""
        events = ", ".join(
            f"*<{self.skyportal_url}/gcn_events/{dateobs}|{skymap.alias}>* "
            f"({skymap.type or 'event'} {dateobs[:16].replace('T', ' ')} UTC, first detection {first_detection_delay(obj, skymap)})"
            for dateobs, skymap in matching_skymaps.items()
        )
        return f"*<{self.skyportal_url}/source/{obj['objectId']}|{obj['objectId']}>* in {events}\n{photometry_summary(obj)}"

    def send(self, obj, matching_skymaps, pending_review=False):
        """
        Post the detection with its skymap plots and return the timestamp of the message, None if Slack did not give it.
        A plot that fails is only logged, so that the detection is posted without it rather than not at all.
        """
        text = self.header(obj, matching_skymaps) + ("\n_Analysis running..._" if pending_review else "")
        plots = []
        for skymap in matching_skymaps.values():
            try:
                plots.append((f"{obj['objectId']}_{skymap.alias}.png", plot_object_on_skymap(obj, skymap).getvalue()))
            except Exception as e:
                log(f"{RED}Could not plot {obj['objectId']} on the {skymap.alias} skymap:{ENDC} {e}")
        if plots:
            try:
                return self._share_ts(self._upload(plots, initial_comment=text))
            except Exception as e:
                log(f"{RED}Could not upload the skymap plots of {obj['objectId']} to Slack, its detection is posted without them:{ENDC} {e}")
        return self.post(text)

    def send_details(self, ts, obj, matching_skymaps, gcn_payload, notes=None):
        """Distance checks and GCN notice payload in the thread of the detection message."""
        notes_text = "".join(f"*Distance:* {note}\n" for note in notes or [])
        self._upload(
            [(f"gcn_notice_payload_{obj['objectId']}_{'_'.join(skymap.alias for skymap in matching_skymaps.values())}.json",
              json.dumps(gcn_payload, indent=2, ensure_ascii=False).encode("utf-8"))],
            initial_comment=f"{notes_text}*GCN notice payload:*",
            thread_ts=ts,
        )


def first_detection_delay(obj, skymap):
    """Time from the trigger to the first detection, e.g. "+2.6 d" or "+5 h"."""
    days = next(phot["jd"] for phot in obj["filtered_photometry"] if phot["flux"] is not None) - skymap.jd
    return f"+{days:.1f} d" if days >= 1 else f"+{days * 24:.0f} h"


def photometry_summary(obj):
    """Number of detections per survey and the last one, e.g. "3 detections, ZTF 2 + LSST 1, last r 19.31 (LSST)"."""
    detections = [phot for phot in obj["filtered_photometry"] if phot["flux"] is not None]
    surveys = Counter(SURVEY_NAMES.get(get_survey(obj, phot)["instrument"], get_survey(obj, phot)["instrument"]) for phot in detections)
    split = next(iter(surveys)) + " only" if len(surveys) == 1 else " + ".join(f"{name} {n}" for name, n in surveys.items())
    last = detections[-1]
    last_survey = get_survey(obj, last)
    mag = flux_to_mag(last["flux"], last_survey["zp"])
    plural = "s" if len(detections) > 1 else ""
    where = f" ({SURVEY_NAMES.get(last_survey['instrument'], last_survey['instrument'])})" if len(surveys) > 1 else ""
    return f"{len(detections)} detection{plural}, {split}, last {last['band'].removeprefix('ztf')} {mag:.2f}{where}"
