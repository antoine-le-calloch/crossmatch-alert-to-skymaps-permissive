import os

from dotenv import load_dotenv
from astropy.time import Time

from utils.logger import log, YELLOW, GREEN, ENDC
from utils.converter import flux_to_mag, flux_err_to_mag_error, flux_err_to_limiting_mag, str_to_bool

load_dotenv()

SCHEMA = "https://gcn.nasa.gov/schema/v7.0.0/gcn/notices/boom/alert.schema.json"


def get_gcn_kafka_config(testing_mode=None):
    """
    Get Kafka configuration for GCN connection based on environment variables.

    Parameters
    ----------
    testing_mode : bool, optional
        Whether to use testing mode configuration (GCN_KAFKA_TEST_*).
        If None, it will be determined by the GCN_KAFKA_TESTING_MODE environment variable (default: True).

    Returns
    -------
    dict
        A dictionary containing Kafka configuration parameters: client_id, client_secret, domain, topic, and heartbeat_topic.
    """
    if testing_mode is None:
        testing_mode = str_to_bool(os.getenv("GCN_KAFKA_TESTING_MODE"), default=True)
    if testing_mode:
        log(f"{YELLOW}GCN Kafka is running in TESTING MODE using test server and credentials.{ENDC}")
    else:
        log(f"{GREEN}GCN Kafka is running in PRODUCTION MODE using production server and credentials.{ENDC}")

    prefix = "GCN_KAFKA_TEST_" if testing_mode else "GCN_KAFKA_"
    values = {v: os.getenv(prefix + v) for v in ("USERNAME", "PASSWORD", "SERVER", "TOPIC")}
    missing = [prefix + v for v, val in values.items() if not val]
    if missing:
        raise RuntimeError(f"Missing required GCN Kafka environment variable(s): {', '.join(missing)}")

    return {
        "client_id": values["USERNAME"],
        "client_secret": values["PASSWORD"],
        "domain": values["SERVER"],
        "topic": values["TOPIC"],
        "heartbeat_topic": f"{values['TOPIC']}.heartbeat",
    }


def prepare_gcn_payload(obj, matching_skymaps):
    payload = {
        '$schema': SCHEMA,
        "alert_datetime": Time.now().isot + "Z",
        "mission": "Boom",
        "data": {
            "targets": [
                {
                    "event_name": obj["objectId"],
                    "ra": obj["ra"],
                    "dec": obj["dec"],
                    "classification_scores": {
                        classification["classifier"]: classification["score"]
                        for classification in obj.get("classifications", [])
                    },
                    "gcn_crossmatch":  [{
                        "ref_type": skymap.type,
                        "ref_instrument": skymap.instrument,
                        "ref_ID": skymap.id,
                    } for skymap in matching_skymaps.values()],
                }
            ],
            "photometry": [{
                "event_name": obj["objectId"],
                "observation_start": Time(p["jd"], format="jd", precision=3).isot + "Z",
                "telescope": "Palomar 1.2m Oschin",
                "instrument": "ZTF",
                "filter": p["band"],
                **(
                    {
                        "mag": round(flux_to_mag(p["flux"]), 2),
                        "mag_error": round(flux_err_to_mag_error(p["flux"], p["flux_err"]), 2),
                    } if p["flux"] and p["flux_err"] else {}
                ),
                "mag_system": "AB",
                "limiting_mag": round(flux_err_to_limiting_mag(p["flux_err"]), 2),
            } for p in obj["filtered_photometry"]]
        },
    }
    return payload