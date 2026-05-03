import math

from astropy.time import Time
from datetime import datetime, timedelta, UTC

# BOOM stores ZTF flux as mag2flux(mag, 23.9) * 1e9 (see boom/src/alert/ztf.rs),
# so we need to adjust the zero point accordingly.
BOOM_ZTF_FLUX_ZP = 23.9 + 2.5 * math.log10(1e9)
_FACTOR = 2.5 / math.log(10)

def flux_to_mag(flux, zp=BOOM_ZTF_FLUX_ZP):
    """Convert flux to AB magnitude."""
    mag = -2.5 * math.log10(flux) + zp
    return mag


def flux_err_to_mag_error(flux, flux_err):
    """Convert flux error to AB magnitude error."""
    return _FACTOR * (flux_err / flux)


def flux_err_to_limiting_mag(flux_err, zp=BOOM_ZTF_FLUX_ZP):
    """5-sigma AB limiting magnitude from flux_err."""
    return -2.5 * math.log10(5.0 * flux_err) + zp


def fallback(hours=0, seconds=0, date_format=None):
    """Get a fallback date by subtracting a specified amount of time from the current UTC time.

    Parameters
    ----------
    hours : int, optional
        The number of hours to subtract from the current time (default is 0).
    seconds : int, optional
        The number of seconds to subtract from the current time (default is 0).
    date_format : str, optional
        The format in which to return the date (default is None, which returns a datetime object).
        If "iso", returns an ISO 8601 string.
        If "mjd", returns the Modified Julian Date.
        If "jd", returns the Julian Date.

    Returns
    -------
    datetime or str or float
        The fallback date in the specified format.
    """
    date = datetime.now(UTC) - timedelta(hours=hours, seconds=seconds)
    if date_format == "iso":
        return date.isoformat()
    if date_format == "mjd":
        return Time(date).mjd
    if date_format == "jd":
        return Time(date).jd
    return date


def str_to_bool(value, default=None):
    """
    Convert a string to a boolean value.

    Accepts various string representations:
        - "yes", "y", "TRUE", "True", "true", "t", "1" => True
        - "no", "n", "FALSE", "False", "false", "f", "0" => False

    If the value is None, empty, or invalid:
        - returns the default if provided
        - raises ValueError otherwise

    Parameters
    ----------
    value : str
        The string to convert to a boolean.
    default : bool, optional
        Value to return if the input is missing or invalid.

    Returns
    -------
    bool
        The converted boolean value.

    Raises
    -------
    ValueError
        If the value is invalid and no default is provided.
    """
    try:
        value_str = str(value).strip().lower()
        if value_str in ("yes", "y", "true", "t", "1"):
            return True
        if value_str in ("no", "n", "false", "f", "0"):
            return False
    except Exception:
        pass  # ignore any conversion error

    if default is not None:
        return default
    raise ValueError(f"Invalid string value for boolean conversion: {value}")
