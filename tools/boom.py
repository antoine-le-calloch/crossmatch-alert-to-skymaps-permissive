import argparse
import gzip
import io
import json
import math
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from utils.converter import BOOM_ZTF_FLUX_ZP, BOOM_LSST_FLUX_ZP

EARTH_RADIUS_M = 6378137.0
MAX_TIME_MS = 60000
CATALOGS = (
    "2MASS_PSC", "AllWISE", "CatWISE2020", "DESI_DR1", "GALEX", "Gaia_DR3", "LSDR10", "LSPSC",
    "MPC_orbits", "NED", "PS1_DR2", "TNS", "VSX", "milliquas_v8",
)
ALERTS = tuple(f"{survey}_alerts{suffix}" for survey in ("ZTF", "LSST") for suffix in ("", "_aux", "_cutouts"))
COLLECTIONS = CATALOGS + ALERTS
FORBIDDEN_STAGES = ("$out", "$merge", "$lookup", "$unionWith", "$graphLookup")
CANDIDATE_KEYS = (
    "candid", "jd", "ra", "dec", "magpsf", "sigmapsf", "diffmaglim", "isdiffpos", "drb", "rb", "fwhm", "elong", "nbad",
    "classtar", "ndethist", "ncovhist", "jdstarthist", "distnr", "magnr", "sgscore1", "distpsnr1", "srmag1", "sgscore2",
    "distpsnr2", "srmag2", "ssdistnr", "ssmagnr", "ssnamenr", "programid",
    "band", "reliability", "snr_psf", "extendedness", "isDipole", "glint_trail", "pixelFlags_streak", "diaObjectId",
)


def get_db():
    uri = os.getenv("BOOM_MONGO_URI")
    if not uri:
        raise SystemExit("BOOM_MONGO_URI is not set")
    return MongoClient(uri, serverSelectionTimeoutMS=10000).get_default_database()


def get_collection(db, name):
    if name not in COLLECTIONS:
        raise SystemExit(f"Collection {name} is not readable here, use one of: {', '.join(COLLECTIONS)}")
    return db[name]


def survey_of(object_id):
    return "ZTF" if object_id.startswith("ZTF") else "LSST"


def mag(flux, zp):
    return round(-2.5 * math.log10(flux) + zp, 2) if flux and flux > 0 else None


def compact_photometry(aux, zp):
    """Detections, non-detections and forced photometry of an aux document as short rows."""
    detections = [
        {"jd": round(p["jd"], 4), "band": p.get("band"), "mag": round(p["magpsf"], 2), "err": round(p["sigmapsf"], 2),
         "isdiffpos": p.get("isdiffpos"), "real": round(p.get("drb", p.get("reliability")) or 0, 3)}
        for p in aux.get("prv_candidates") or []
    ]
    non_detections = [
        {"jd": round(p["jd"], 4), "band": p.get("band"), "limit": round(p["diffmaglim"], 2)}
        for p in aux.get("prv_nondetections") or [] if p.get("diffmaglim")
    ]
    forced = []
    for p in aux.get("fp_hists") or []:
        flux, flux_err = p.get("psfFlux"), p.get("psfFluxErr")
        if flux is None or not flux_err or flux_err <= 0:
            continue
        snr = flux / flux_err
        forced.append({
            "jd": round(p["jd"], 4), "band": p.get("band"), "snr": round(snr, 1),
            "mag": mag(flux, zp) if snr >= 3 else None, "limit": mag(5 * flux_err, zp),
        })
    return {
        "detections": sorted(detections, key=lambda p: p["jd"]),
        "non_detections": sorted(non_detections, key=lambda p: p["jd"])[-15:],
        "forced_photometry": sorted(forced, key=lambda p: p["jd"]),
    }


def summarize_object(db, object_id):
    survey = survey_of(object_id)
    alerts = get_collection(db, f"{survey}_alerts")
    latest = alerts.find_one({"objectId": object_id}, sort=[("candidate.jd", -1)], max_time_ms=MAX_TIME_MS)
    if not latest:
        raise SystemExit(f"No {survey} alert for {object_id}")
    aux = get_collection(db, f"{survey}_alerts_aux").find_one({"_id": object_id}, max_time_ms=MAX_TIME_MS) or {}
    properties = latest.get("properties") or {}
    host = aux.get("host_galaxy") or {}
    return {
        "object_id": object_id,
        "n_alerts": alerts.count_documents({"objectId": object_id}, maxTimeMS=MAX_TIME_MS),
        "latest_alert": {k: v for k, v in (latest.get("candidate") or {}).items() if k in CANDIDATE_KEYS},
        "galactic": {k: latest.get("coordinates", {}).get(k) for k in ("l", "b")},
        "classifications": latest.get("classifications"),
        "properties": {k: v for k, v in properties.items() if k not in ("photstats", "multisurvey_photstats")},
        "photstats": {band: stats for band, stats in (properties.get("photstats") or {}).items() if stats},
        "photometry": compact_photometry(aux, BOOM_ZTF_FLUX_ZP if survey == "ZTF" else BOOM_LSST_FLUX_ZP),
        "cross_matches": {catalog: matches[:3] for catalog, matches in (aux.get("cross_matches") or {}).items() if matches},
        "host_galaxy": {
            "best_host": host.get("best_host"),
            "p_host_none": host.get("p_host_none"),
            "other_candidates": [c for c in host.get("candidates") or [] if c != host.get("best_host")][:3],
        },
        "aliases": aux.get("aliases"),
    }


def near(db, catalog, ra, dec, radius, limit, projection):
    query = {
        "coordinates.radec_geojson": {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [ra - 180.0, dec]},
                "$maxDistance": math.radians(radius / 3600.0) * EARTH_RADIUS_M,
            }
        }
    }
    matches = list(get_collection(db, catalog).find(query, projection, limit=limit, max_time_ms=MAX_TIME_MS))
    for match in matches:
        lon, lat = match.get("coordinates", {}).get("radec_geojson", {}).get("coordinates", [None, None])
        if lon is not None:
            ra2, dec1, dec2 = math.radians(lon + 180.0), math.radians(dec), math.radians(lat)
            cos_sep = math.sin(dec1) * math.sin(dec2) + math.cos(dec1) * math.cos(dec2) * math.cos(ra2 - math.radians(ra))
            match["distance_arcsec"] = round(math.degrees(math.acos(min(1.0, max(-1.0, cos_sep)))) * 3600, 2)
    return matches


def has_forbidden_stage(value):
    if isinstance(value, dict):
        return any(k in FORBIDDEN_STAGES or has_forbidden_stage(v) for k, v in value.items())
    if isinstance(value, list):
        return any(has_forbidden_stage(v) for v in value)
    return False


def cutouts(db, object_id, which):
    import warnings
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from astropy.io import fits
    from astropy.visualization import ZScaleInterval

    warnings.simplefilter("ignore")
    survey = survey_of(object_id)
    sort = {"first": ("candidate.jd", 1), "last": ("candidate.jd", -1), "brightest": ("candidate.magpsf", 1)}[which]
    alert = get_collection(db, f"{survey}_alerts").find_one({"objectId": object_id}, {"_id": 1, "candidate.jd": 1}, sort=[sort], max_time_ms=MAX_TIME_MS)
    if not alert:
        raise SystemExit(f"No {survey} alert for {object_id}")
    images = get_collection(db, f"{survey}_alerts_cutouts").find_one({"_id": alert["_id"]}, max_time_ms=MAX_TIME_MS)
    if not images:
        raise SystemExit(f"No cutouts for alert {alert['_id']}")
    fig, axes = plt.subplots(1, 3, figsize=(12, 4))
    for ax, key in zip(axes, ("cutoutScience", "cutoutTemplate", "cutoutDifference")):
        raw = images[key]
        with fits.open(io.BytesIO(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)) as hdul:
            image = hdul[0].data
        vmin, vmax = ZScaleInterval().get_limits(image)
        ax.imshow(image, origin="lower", cmap="gray", vmin=vmin, vmax=vmax)
        ax.set_title(key[len("cutout"):])
        ax.axis("off")
    path = Path.cwd() / f"{object_id}_{which}_cutouts.png"
    fig.savefig(path, bbox_inches="tight", dpi=100)
    return {"candid": alert["_id"], "jd": alert["candidate"]["jd"], "png": str(path)}


def main():
    parser = argparse.ArgumentParser(description="Read-only access to the BOOM MongoDB (alerts and catalogs).")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("collections", help="List the readable collections")

    p = sub.add_parser("object", help="Summary of an object: latest alert, scores, photometry, forced photometry, cross-matches, host galaxy")
    p.add_argument("object_id")

    p = sub.add_parser("find", help="Mongo find on a collection")
    p.add_argument("collection")
    p.add_argument("filter", help="JSON filter")
    p.add_argument("--projection", help="JSON projection")
    p.add_argument("--sort", help="JSON sort, e.g. '{\"candidate.jd\": -1}'")
    p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("near", help="Entries of a catalog around a position, closest first, with distance_arcsec")
    p.add_argument("collection")
    p.add_argument("ra", type=float)
    p.add_argument("dec", type=float)
    p.add_argument("radius", type=float, help="arcsec")
    p.add_argument("--projection", help="JSON projection")
    p.add_argument("--limit", type=int, default=5)

    p = sub.add_parser("aggregate", help="Aggregation pipeline on a collection (no $out, $merge, $lookup, $unionWith, $graphLookup)")
    p.add_argument("collection")
    p.add_argument("stages", help="JSON list of stages")

    p = sub.add_parser("cutouts", help="Save the science/template/difference cutouts of an alert as a PNG in the current directory")
    p.add_argument("object_id")
    p.add_argument("--which", choices=["first", "last", "brightest"], default="last")

    args = parser.parse_args()
    db = get_db()
    if args.command == "collections":
        result = list(COLLECTIONS)
    elif args.command == "object":
        result = summarize_object(db, args.object_id)
    elif args.command == "find":
        result = list(get_collection(db, args.collection).find(
            json.loads(args.filter),
            json.loads(args.projection) if args.projection else None,
            sort=list(json.loads(args.sort).items()) if args.sort else None,
            limit=args.limit,
            max_time_ms=MAX_TIME_MS,
        ))
    elif args.command == "near":
        result = near(db, args.collection, args.ra, args.dec, args.radius, args.limit, json.loads(args.projection) if args.projection else None)
    elif args.command == "aggregate":
        stages = json.loads(args.stages)
        if has_forbidden_stage(stages):
            raise SystemExit(f"Refusing a pipeline with one of {', '.join(FORBIDDEN_STAGES)}")
        result = list(get_collection(db, args.collection).aggregate(stages, maxTimeMS=MAX_TIME_MS))
    else:
        result = cutouts(db, args.object_id, args.which)
    print(json.dumps(result, indent=1, default=lambda v: f"<{len(v)} bytes>" if isinstance(v, bytes) else str(v)))


if __name__ == "__main__":
    main()
