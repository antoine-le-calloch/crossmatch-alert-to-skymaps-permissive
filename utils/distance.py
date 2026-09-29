import math
import numpy as np

from utils.converter import flux_to_mag, get_survey

C_OVER_H0_MPC = 299792.458 / 70.0
MIN_HOST_POSTERIOR = 0.5
DISTANCE_QUANTILE_RANGE = (0.005, 0.995)
ABS_MAG_RANGE = (-20.0, -11.0)


def get_host_distance(alert):
    """Distance in Mpc of the alert's best host galaxy, or None if it has no reliable host distance."""
    host = alert.get("host_galaxy")
    if not host or (host.get("posterior") or 0) < MIN_HOST_POSTERIOR:
        return None
    if host.get("dist_mpc"):
        return host["dist_mpc"]
    if host.get("z") and host["z"] > 0:
        return host["z"] * C_OVER_H0_MPC
    return None


def get_line_of_sight_distance(distmu, distsigma):
    """Mean, standard deviation and CDF of the GW distance along a line of sight."""
    r = np.linspace(0, max(distmu, 0) + 6 * distsigma, 4000)[1:]
    pdf = r**2 * np.exp(-0.5 * ((r - distmu) / distsigma) ** 2)
    pdf /= pdf.sum()
    mean = float((r * pdf).sum())
    std = float(math.sqrt(((r - mean) ** 2 * pdf).sum()))
    return mean, std, lambda distance: float(np.interp(distance, r, np.cumsum(pdf)))


def get_peak_mag(alert, filtered_photometry):
    """Brightest AB magnitude of the detections."""
    mags = [
        flux_to_mag(phot["flux"], get_survey(alert, phot)["zp"])
        for phot in filtered_photometry if phot["flux"] is not None and phot["flux"] > 0
    ]
    return min(mags) if mags else None


def get_distance_info(skymap, alert, filtered_photometry):
    """Compare the alert with the distance of a 3D skymap.

    Returns None if the skymap has no distance at the alert position. Otherwise, returns
    the GW distance along the line of sight, the host distance and its quantile in the GW
    distance posterior (if the alert has a reliable host), and the peak absolute magnitude
    at the host distance, or at the GW distance without a host.
    """
    if skymap.distance is None:
        return None
    distance = skymap.distance.at(alert["ra"], alert["dec"])
    if distance is None:
        return None

    gw_mean, gw_std, gw_cdf = get_line_of_sight_distance(*distance)
    host_distance = get_host_distance(alert)
    peak_mag = get_peak_mag(alert, filtered_photometry)
    reference_distance = host_distance or gw_mean
    return {
        "gw_distance_mpc": gw_mean,
        "gw_distance_std_mpc": gw_std,
        "host_distance_mpc": host_distance,
        "host_distance_quantile": gw_cdf(host_distance) if host_distance else None,
        "abs_mag": peak_mag - 5 * math.log10(reference_distance * 1e5) if peak_mag and reference_distance > 0 else None,
    }


def is_distance_consistent(distance_info):
    """False when the host distance is outside the GW distance posterior or the absolute magnitude is implausible."""
    if distance_info is None:
        return True
    quantile = distance_info["host_distance_quantile"]
    if quantile is not None and not DISTANCE_QUANTILE_RANGE[0] <= quantile <= DISTANCE_QUANTILE_RANGE[1]:
        return False
    abs_mag = distance_info["abs_mag"]
    return abs_mag is None or ABS_MAG_RANGE[0] <= abs_mag <= ABS_MAG_RANGE[1]


def format_distance_info(distance_info):
    """One line summary of the distance comparison, for Slack."""
    parts = [f"GW distance: {distance_info['gw_distance_mpc']:.0f} ± {distance_info['gw_distance_std_mpc']:.0f} Mpc"]
    if distance_info["host_distance_mpc"]:
        parts.append(f"host: {distance_info['host_distance_mpc']:.0f} Mpc (quantile {distance_info['host_distance_quantile']:.3f})")
    if distance_info["abs_mag"] is not None:
        parts.append(f"peak M: {distance_info['abs_mag']:.1f}")
    return ", ".join(parts)
