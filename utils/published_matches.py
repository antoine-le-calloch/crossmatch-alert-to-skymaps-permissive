import json
import os
from pathlib import Path

from utils.logger import log, YELLOW, ENDC

PUBLISHED_MATCHES_FILE = Path(__file__).resolve().parent.parent / "published_matches.json"


def load_published_matches(path=PUBLISHED_MATCHES_FILE):
    """
    Load the matches saved by a previous run, so that replaying the Kafka topic after a restart
    does not publish them again.

    Returns
    -------
    dict
        {objectId: {"skymaps": set((dateobs, created_at)), "first_detection_jd": float}}
    """
    try:
        with open(path) as f:
            saved = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as e:
        log(f"{YELLOW}Could not read {path}, starting without the published matches of the previous run:{ENDC} {e}")
        return {}
    return {
        obj_id: {"skymaps": {tuple(skymap) for skymap in info["skymaps"]}, "first_detection_jd": info["first_detection_jd"]}
        for obj_id, info in saved.items()
    }


def save_published_matches(published_matches, path=PUBLISHED_MATCHES_FILE):
    """Write the published matches to disk, atomically."""
    serializable = {
        obj_id: {"skymaps": sorted(info["skymaps"]), "first_detection_jd": info["first_detection_jd"]}
        for obj_id, info in published_matches.items()
    }
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(serializable, f)
    os.replace(tmp_path, path)
