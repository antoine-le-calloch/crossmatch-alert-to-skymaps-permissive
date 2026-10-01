import io
import json
import os
import time
import urllib.error

from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from datetime import datetime, UTC

from utils.logger import log, YELLOW, ENDC
from utils.skymap import plot_object_on_skymap

load_dotenv()

UPLOAD_ATTEMPTS = 3
UPLOAD_RETRY_DELAY = 10  # seconds, doubled after each failed attempt


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

    def _upload(self, content, **kwargs):
        """files_upload_v2 in the channel, retried on transient errors."""
        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            try:
                return self.client.files_upload_v2(channel=self.channel_id, file=io.BytesIO(content), **kwargs)
            except Exception as e:
                if attempt == UPLOAD_ATTEMPTS or not is_transient(e):
                    raise
                delay = UPLOAD_RETRY_DELAY * 2 ** (attempt - 1)
                log(f"{YELLOW}Slack upload failed ({e}), retrying in {delay} seconds{ENDC}")
                time.sleep(delay)

    def send(self, obj, matching_skymaps, gcn_payload, notes=None):
        """Send a message to Slack with the object details, optional notes and crossmatch plots."""
        notes_text = "".join(f"*Distance:* {note}\n" for note in notes or [])
        self._upload(
            json.dumps(gcn_payload, indent=2, ensure_ascii=False).encode("utf-8"),
            initial_comment=(
                f"*New object in Skymaps localization*\n"
                f"*Date:* {datetime.now(UTC).replace(microsecond=0).isoformat()} UTC\n"
                f"*Object:* <{self.skyportal_url}/source/{obj['objectId']}|{obj['objectId']}>\n"
                f"{notes_text}"
                f"*GCN notice payload:*"
            ),
            title=f"gcn_notice_payload_{obj['objectId']}_{'_'.join([skymap.alias for skymap in matching_skymaps.values()])}.json",
        )

        for dateobs, skymap in matching_skymaps.items():
            self._upload(
                plot_object_on_skymap(obj, skymap).getvalue(),
                filename=f"{obj['objectId']}_{skymap.alias}.png",
                initial_comment=f"*Alias:* <{self.skyportal_url}/gcn_events/{dateobs}|{skymap.alias}>",
            )
