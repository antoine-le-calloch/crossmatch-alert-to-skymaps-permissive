import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

from utils.api import SkyPortal, APIError

load_dotenv(ROOT / ".env")

USAGE = """Read-only access to the SkyPortal (Fritz) API, GET requests only.

usage: fritz.py ENDPOINT [KEY=VALUE ...]

examples:
  fritz.py /api/sources/ZTF26aaaaaaa includeComments=true includeClassifications=true
  fritz.py /api/sources/ZTF26aaaaaaa/photometry format=mag
  fritz.py /api/candidates/ZTF26aaaaaaa
  fritz.py /api/sources/ZTF26aaaaaaa/annotations
  fritz.py /api/sources/ZTF26aaaaaaa/spectra
  fritz.py /api/sources ra=123.4 dec=-5.6 radius=0.002
  fritz.py /api/gcn_event/2026-09-28T12:34:56 excludeNoticeContent=true"""


def main():
    if len(sys.argv) < 2 or not sys.argv[1].startswith("/api/"):
        raise SystemExit(USAGE)
    params = dict(arg.split("=", 1) for arg in sys.argv[2:])
    skyportal = SkyPortal(instance=os.getenv("SKYPORTAL_URL"), token=os.getenv("SKYPORTAL_API_KEY"), validate=False)
    try:
        data = skyportal.api("GET", sys.argv[1], data=params)
    except APIError as e:
        raise SystemExit(str(e))
    print(json.dumps(data, indent=1, default=str))


if __name__ == "__main__":
    main()
