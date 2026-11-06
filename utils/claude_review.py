import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, UTC
from pathlib import Path

from dotenv import load_dotenv

from utils.logger import log, RED, YELLOW, ENDC

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
SYSTEM_PROMPT = ROOT / "prompts" / "claude_review.md"
REVIEWS_DIR = ROOT / "reviews"
SESSIONS_FILE = REVIEWS_DIR / "sessions.json"
CLAUDE_BIN = os.getenv("CLAUDE_BIN") or "claude"
CLAUDE_MODEL = os.getenv("CLAUDE_REVIEW_MODEL")
CLAUDE_TIMEOUT = int(os.getenv("CLAUDE_REVIEW_TIMEOUT") or 900)
CLAUDE_WORKERS = int(os.getenv("CLAUDE_REVIEW_WORKERS") or 1)
THREAD_POLL_INTERVAL = int(os.getenv("CLAUDE_THREAD_POLL_INTERVAL") or 30)
THREAD_MAX_AGE_DAYS = 14
HEADER_PREFIX = "*Analysis of "
TOOL_COMMANDS = {name: f"{sys.executable} {ROOT / 'tools' / f'{name}.py'}" for name in ("fritz", "boom")}
WEB_DOMAINS = ("gcn.nasa.gov", "www.wis-tns.org", "simbad.cds.unistra.fr", "vizier.cds.unistra.fr", "ned.ipac.caltech.edu", "minorplanetcenter.net")
HEAVY_ALERT_KEYS = ("photometry", "filtered_photometry", "survey_matches", "cutoutScience", "cutoutTemplate", "cutoutDifference")


def build_context(alert, matching_skymaps, gcn_payload, notes):
    skyportal_url = os.getenv("SKYPORTAL_URL")
    return {
        "object_id": alert["objectId"],
        "fritz_url": f"{skyportal_url}/source/{alert['objectId']}",
        "skymaps": [
            {
                "alias": skymap.alias,
                "dateobs": dateobs,
                "type": skymap.type,
                "area_90_sq_deg": skymap.area_90,
                "localization_created_at": skymap.created_at,
                "url": f"{skyportal_url}/gcn_events/{dateobs}",
            }
            for dateobs, skymap in matching_skymaps.items()
        ],
        "distance_notes": notes,
        "photometry_last_non_detection_and_detections": gcn_payload["data"]["photometry"],
        "survey_matches": {
            survey: {k: v for k, v in match.items() if k != "photometry"}
            for survey, match in (alert.get("survey_matches") or {}).items() if match
        },
        "alert": {k: v for k, v in alert.items() if k not in HEAVY_ALERT_KEYS},
    }


def header(context):
    aliases = ", ".join(skymap.get("alias") or skymap["dateobs"] for skymap in context["skymaps"])
    return f"{HEADER_PREFIX}<{context['fritz_url']}|{context['object_id']}>* ({aliases})"


def parse(text):
    """Split an analysis into the JSON head ({} when missing or invalid) and the details."""
    match = re.search(r"```json\s*(\{.*?\})\s*```", text, re.S)
    if not match:
        return {}, text.strip()
    try:
        head = json.loads(re.sub(r",\s*([}\]])", r"\1", match.group(1)))
    except json.JSONDecodeError:
        return {}, text.strip()
    return head, (text[:match.start()] + text[match.end():]).strip()


def bar(percent, width=20):
    filled = round(max(0, min(percent, 100)) / 100 * width)
    return "█" * filled + "░" * (width - filled)


def verdict(head, details):
    """Head of the message: Claude's summary, its classes with their probability bars and the decisive reason."""
    if not head.get("summary"):
        return f"*Analysis:*\n{details}"
    tags = " · ".join(f"{key} {head[key]}" for key in ("priority", "confidence") if head.get(key))
    lines = [f"*Verdict: {head['summary']}*" + (f"\n{tags}" if tags else "")]
    classes = [(str(name), float(percent)) for name, percent in head.get("classes") or []]
    if classes:
        width = min(max(len(name) for name, _ in classes), 28)
        lines.append("```" + "\n".join(f"{name[:width]:<{width}}  {bar(percent)}  {percent:3.0f}%" for name, percent in classes) + "```")
    if head.get("reason"):
        lines.append(f"_{head['reason']}_")
    return "\n".join(lines)


def task_footer():
    return (
        f"Fritz tool command: `{TOOL_COMMANDS['fritz']}`\n"
        f"BOOM tool command: `{TOOL_COMMANDS['boom']}`\n"
        f"Current time: {datetime.now(UTC).replace(microsecond=0).isoformat()}"
    )


def new_workdir(name):
    workdir = REVIEWS_DIR / f"{name}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%S%f')}"
    workdir.mkdir(parents=True)
    return workdir


def run_claude(prompt, workdir, session_id=None):
    """Run Claude Code headless in workdir with the subscription login, resuming session_id if given, return its answer and session id."""
    allowed_tools = [
        f"Read(/{workdir}/**)",
        *(f"Bash({command} *)" for command in TOOL_COMMANDS.values()),
        *(f"WebFetch(domain:{domain})" for domain in WEB_DOMAINS),
        "WebSearch",
    ]
    command = [
        CLAUDE_BIN, "--print",
        *(["--resume", session_id] if session_id else []),
        *(["--model", CLAUDE_MODEL] if CLAUDE_MODEL else []),
        "--output-format", "json",
        "--append-system-prompt-file", str(SYSTEM_PROMPT),
        "--permission-mode", "dontAsk",
        "--allowedTools", *allowed_tools,
    ]
    env = {k: v for k, v in os.environ.items() if k not in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")}
    process = subprocess.run(command, input=prompt, capture_output=True, text=True, cwd=workdir, env=env, timeout=CLAUDE_TIMEOUT)
    try:
        result = json.loads(process.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(f"claude exited with code {process.returncode}: {(process.stderr or process.stdout)[-500:]}")
    if result.get("is_error") or result.get("subtype") != "success":
        raise RuntimeError(f"{result.get('subtype')}: {str(result.get('result'))[-500:]}")
    return result["result"].strip(), result["session_id"]


def review(context):
    """Analyse a detection, return the Slack message and the session to answer questions about it later."""
    workdir = new_workdir(context["object_id"])
    prompt = (
        "Analyse this candidate counterpart.\n\n"
        f"{task_footer()}\n\n"
        f"Detection context from the pipeline:\n```json\n{json.dumps(context, indent=1, default=str)}\n```"
    )
    text, session_id = run_claude(prompt, workdir)
    return text, {"session_id": session_id, "workdir": str(workdir), "object_id": context["object_id"]}


def answer(analysis, questions, session=None):
    """Answer messages posted in the thread of an analysis, resuming its session when it still exists."""
    if session and session.get("session_id") and Path(session["workdir"]).is_dir():
        prompt = f"New message(s) in the Slack thread of your analysis:\n{questions}\n\n{task_footer()}"
        try:
            text, session_id = run_claude(prompt, session["workdir"], session["session_id"])
            return text, {**session, "session_id": session_id}
        except RuntimeError as e:
            log(f"{YELLOW}Could not resume Claude session {session['session_id']}, starting a new one:{ENDC} {e}")

    object_id = (session or {}).get("object_id")
    if not object_id:
        match = re.search(r"\|([^>|]+)>", analysis)
        object_id = match.group(1) if match else "thread"
    workdir = new_workdir(object_id)
    prompt = (
        f"You posted this analysis in Slack, but the session in which you wrote it is lost:\n{analysis}\n\n"
        f"New message(s) in its thread:\n{questions}\n\n"
        "Check again with the tools whatever you need to answer.\n\n"
        f"{task_footer()}"
    )
    text, session_id = run_claude(prompt, workdir)
    return text, {"session_id": session_id, "workdir": str(workdir), "object_id": object_id}


class Sessions:
    """Claude session of each analysis posted on Slack, by message ts, saved on disk to survive restarts."""

    def __init__(self):
        self.lock = threading.Lock()
        try:
            self.data = json.loads(SESSIONS_FILE.read_text())
        except FileNotFoundError:
            self.data = {}

    def get(self, ts):
        with self.lock:
            return self.data.get(ts)

    def set(self, ts, session):
        with self.lock:
            self.data[ts] = session
            self._save()

    def prune(self):
        """Forget the sessions of analyses older than THREAD_MAX_AGE_DAYS and delete old working directories."""
        cutoff = time.time() - THREAD_MAX_AGE_DAYS * 86400
        with self.lock:
            expired = [ts for ts in self.data if float(ts) < cutoff]
            for ts in expired:
                del self.data[ts]
            if expired:
                self._save()
            kept = {session["workdir"] for session in self.data.values()}
        for workdir in REVIEWS_DIR.iterdir() if REVIEWS_DIR.is_dir() else []:
            if workdir.is_dir() and str(workdir) not in kept and workdir.stat().st_mtime < cutoff:
                shutil.rmtree(workdir, ignore_errors=True)

    def _save(self):
        SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = SESSIONS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=1))
        tmp.replace(SESSIONS_FILE)


class ClaudeReviewer:
    """
    Runs Claude on each published detection in a background thread and posts its analysis in the channel,
    then answers the messages people post in the thread of each analysis.
    """

    def __init__(self, slack):
        self.slack = slack
        self.sessions = Sessions()
        self.review_executor = ThreadPoolExecutor(max_workers=CLAUDE_WORKERS, thread_name_prefix="claude_review")
        self.answer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="claude_answer")
        self.bot_id = slack.client.auth_test()["bot_id"]
        self.in_flight = set()
        self.latest_replies = {}
        threading.Thread(target=self._watch_threads, name="claude_threads", daemon=True).start()

    def submit(self, alert, matching_skymaps, gcn_payload, notes, ts):
        """Review a detection already posted at ts, then complete its message and thread."""
        context = build_context(alert, matching_skymaps, gcn_payload, notes)
        message = self.slack.header(alert, matching_skymaps)
        self.review_executor.submit(self._review, context, ts, message, (alert, matching_skymaps, gcn_payload, notes))

    def _review(self, context, ts, message, detection):
        obj_id = context["object_id"]
        log(f"Claude review of {obj_id} started")
        session = {"object_id": obj_id, "workdir": "", "session_id": None}
        try:
            text, session = review(context)
            head, details = parse(text)
            log(f"Claude review of {obj_id} done")
        except Exception as e:
            log(f"{RED}Claude review of {obj_id} failed:{ENDC} {e}")
            head, details = {}, f"_Analysis failed: {e}_"
        try:
            message = f"{message}\n{verdict(head, details) if head else details}"
            if ts:
                self.slack.update(ts, message)
            else:
                ts = self.slack.post(message)
            self.sessions.set(ts, session)
            if head:
                self.slack.reply(ts, f"*Analysis:*\n{details}")
        except Exception as e:
            log(f"{RED}Could not post the Claude review of {obj_id} on Slack:{ENDC} {e}")
        try:
            self.slack.send_details(ts, *detection)
        except Exception as e:
            log(f"{RED}Could not post the details of {obj_id} on Slack:{ENDC} {e}")

    def _watch_threads(self):
        while True:
            try:
                self.sessions.prune()
                self._check_threads()
            except Exception as e:
                log(f"{RED}Could not check the threads of the Claude analyses on Slack:{ENDC} {e}")
            time.sleep(THREAD_POLL_INTERVAL)

    def _check_threads(self):
        """Queue an answer for each analysis thread whose last human messages have no bot reply after them."""
        oldest = time.time() - THREAD_MAX_AGE_DAYS * 86400
        history = self.slack.client.conversations_history(channel=self.slack.channel_id, oldest=str(oldest), limit=200)
        for parent in history["messages"]:
            if parent.get("bot_id") != self.bot_id or self.sessions.get(parent["ts"]) is None:
                continue
            latest_reply = parent.get("latest_reply")
            if not latest_reply or parent["ts"] in self.in_flight or self.latest_replies.get(parent["ts"]) == latest_reply:
                continue
            replies = self.slack.client.conversations_replies(channel=self.slack.channel_id, ts=parent["ts"], limit=200)
            pending = []
            for message in replies["messages"][1:]:
                if message.get("bot_id"):
                    pending = []
                elif message.get("subtype") in (None, "thread_broadcast"):
                    pending.append(message)
            self.latest_replies[parent["ts"]] = latest_reply
            if pending:
                self.in_flight.add(parent["ts"])
                self.answer_executor.submit(self._answer, parent, pending)

    def _answer(self, parent, pending):
        users = sorted({message["user"] for message in pending if message.get("user")})
        questions = "\n".join(f"<@{message.get('user')}>: {message.get('text', '')}" for message in pending)
        log(f"Claude answering {len(pending)} message(s) in the thread of {parent['ts']}")
        try:
            text, session = answer(parent["text"], questions, self.sessions.get(parent["ts"]))
            self.sessions.set(parent["ts"], session)
        except Exception as e:
            log(f"{RED}Claude could not answer in the thread of {parent['ts']}:{ENDC} {e}")
            text = f"_Could not answer: {e}_"
        try:
            self.slack.reply(parent["ts"], f"{' '.join(f'<@{user}>' for user in users)} {text}".strip())
        except Exception as e:
            log(f"{RED}Could not post the Claude answer in the thread of {parent['ts']}:{ENDC} {e}")
        finally:
            self.in_flight.discard(parent["ts"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Claude review of a past detection and print it.")
    parser.add_argument("object_id")
    parser.add_argument("dateobs", help="dateobs of the matching GCN event")
    parser.add_argument("--alias", help="alias of the GCN event, e.g. FERMI#bn260926977")
    parser.add_argument("--post", action="store_true", help="also post it in the Slack channel, where the pipeline answers questions in its thread")
    args = parser.parse_args()
    context = {
        "object_id": args.object_id,
        "fritz_url": f"{os.getenv('SKYPORTAL_URL')}/source/{args.object_id}",
        "skymaps": [{"alias": args.alias, "dateobs": args.dateobs}],
        "note": "Manual run on a past detection: the detection details are not included, fetch them with the tools.",
    }
    text, session = review(context)
    head, details = parse(text)
    text = f"{header(context)}\n{verdict(head, details)}"
    print(f"{text}\n\n{details}" if head else text)
    if args.post:
        from utils.slack import SlackNotifier
        slack = SlackNotifier()
        ts = slack.post(text)
        Sessions().set(ts, session)
        if head:
            slack.reply(ts, f"*Analysis:*\n{details}")
