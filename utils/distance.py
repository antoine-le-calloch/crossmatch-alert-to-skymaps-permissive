import math
import numpy as np

from utils.converter import flux_to_mag, get_survey

C_OVER_H0_MPC = 299792.458 / 70.0
MIN_HOST_POSTERIOR = 0.5
DISTANCE_QUANTILE_RANGE = (0.005, 0.995)
ABS_MAG_RANGE = (-20.0, -11.0)
HOST_Z_ERR_SIGMAS = 2.0


def get_host_distance(alert):
    """Distance in Mpc of the alert's best host galaxy with its lowest and highest values, or None if it has no reliable host distance."""
    host = alert.get("host_galaxy")
    if not host or (host.get("posterior") or 0) < MIN_HOST_POSTERIOR:
        return None
    if host.get("dist_mpc"):
        return host["dist_mpc"], host["dist_mpc"], host["dist_mpc"]
    z = host.get("z")
    if z and z > 0:
        z_err = HOST_Z_ERR_SIGMAS * (host.get("z_err") or 0)
        return z * C_OVER_H0_MPC, max(z - z_err, 0) * C_OVER_H0_MPC, (z + z_err) * C_OVER_H0_MPC
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


def get_abs_mag(peak_mag, distance):
    """Absolute magnitude of an apparent magnitude at a distance in Mpc."""
    return peak_mag - 5 * math.log10(distance * 1e5) if peak_mag and distance > 0 else None


def get_distance_info(skymap, alert, filtered_photometry):
    """Compare the alert with the distance of a 3D skymap.

    Returns None if the skymap has no distance at the alert position. Otherwise, returns
    the GW distance along the line of sight, the host distance, its range from the redshift
    error and their quantiles in the GW distance posterior (if the alert has a reliable host),
    and the peak absolute magnitude at the host distance, or at the GW distance without a host.
    """
    if skymap.distance is None:
        return None
    distance = skymap.distance.at(alert["ra"], alert["dec"])
    if distance is None:
        return None

    gw_mean, gw_std, gw_cdf = get_line_of_sight_distance(*distance)
    host = get_host_distance(alert)
    host_distance, host_min, host_max = host or (None, None, None)
    peak_mag = get_peak_mag(alert, filtered_photometry)
    abs_mag = get_abs_mag(peak_mag, host_distance or gw_mean)
    return {
        "gw_distance_mpc": gw_mean,
        "gw_distance_std_mpc": gw_std,
        "host_distance_mpc": host_distance,
        "host_distance_range_mpc": (host_min, host_max) if host else None,
        "host_distance_quantile": gw_cdf(host_distance) if host else None,
        "host_distance_quantile_range": (gw_cdf(host_min), gw_cdf(host_max)) if host else None,
        "abs_mag": abs_mag,
        "abs_mag_range": (get_abs_mag(peak_mag, host_max), get_abs_mag(peak_mag, host_min)) if host else (abs_mag, abs_mag),
    }


def is_distance_consistent(distance_info):
    """False when the whole host distance range is outside the GW distance posterior or gives an implausible absolute magnitude."""
    if distance_info is None:
        return True
    quantiles = distance_info["host_distance_quantile_range"]
    if quantiles and (quantiles[1] < DISTANCE_QUANTILE_RANGE[0] or quantiles[0] > DISTANCE_QUANTILE_RANGE[1]):
        return False
    brightest, faintest = distance_info["abs_mag_range"]
    if brightest is not None and brightest > ABS_MAG_RANGE[1]:
        return False
    return faintest is None or faintest >= ABS_MAG_RANGE[0]


def format_distance_info(distance_info):
    """One line summary of the distance comparison, for Slack."""
    parts = [f"GW distance: {distance_info['gw_distance_mpc']:.0f} ± {distance_info['gw_distance_std_mpc']:.0f} Mpc"]
    if distance_info["host_distance_mpc"]:
        low, high = distance_info["host_distance_range_mpc"]
        spread = f"{low:.0f} to {high:.0f} Mpc, " if high > low else ""
        parts.append(f"host: {distance_info['host_distance_mpc']:.0f} Mpc ({spread}quantile {distance_info['host_distance_quantile']:.3f})")
    if distance_info["abs_mag"] is not None:
        parts.append(f"peak M: {distance_info['abs_mag']:.1f}")
    return ", ".join(parts)
