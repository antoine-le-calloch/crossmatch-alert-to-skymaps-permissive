import io
import numpy as np
import astropy.units as u
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

from mocpy import MOC
from dataclasses import dataclass, field
from astropy.time import Time
from astropy_healpix import HEALPix
from astropy.wcs import WCS
from astropy.io import fits
from astropy.visualization.wcsaxes.frame import EllipticalFrame

from utils.logger import log


@dataclass
class Skymap:
    """A Skymap represents a localization region for a GCN event, defined by a MOC and associated metadata.

    Attributes
    ----------
    dateobs : str
        The date the event was detected, in ISO format (e.g., "2024-06-01T12:34:56Z").
    alias : str
        The alias of the event, typically in the format "instrument#id" (e.g., "LVC#S200115j").
    moc : MOC
        The MOC object representing the last localization region for the event.
    created_at : str
        The timestamp when the last localization was created, in ISO format (e.g., "2024-06-01T13:00:00Z").
    tags : list[str]
        A list of tags associated with the event, such as "GW", "GRB", "SVOM" or "Einstein Probe"
    jd : float
        The Julian Date corresponding to dateobs.
    """
    dateobs: str
    alias: str
    moc: MOC
    created_at: str
    tags: list[str]
    jd: float = field(init=False)

    def __post_init__(self):
        """Calculate the Julian Date from dateobs after initialization."""
        self.jd = Time(self.dateobs).jd

    @property
    def name(self):
        """Generate a name for the skymap based on its alias and creation time."""
        return f"{self.alias}/{self.created_at}"

    @property
    def type(self):
        """Determine the type of event based on its tags."""
        if self.tags:
            if "GW" in self.tags:
                return "GW"
            elif any(tag in ["GRB", "SVOM"] for tag in self.tags):
                return "GRB"
            elif "Einstein Probe" in self.tags:
                return "XRay"
        return None

    @property
    def instrument(self):
        """Extract the instrument name from the alias"""
        prefix = self.alias.split("#")[0].upper()
        return "LVK" if prefix == "LVC" else prefix

    @property
    def id(self):
        """Extract the event ID from the alias, if present."""
        return self.alias.split("#")[1] if "#" in self.alias else None

    def contains(self, ra, dec):
        """Check if the given (ra, dec) coordinates are contained within the MOC."""
        return self.moc.contains_lonlat(ra * u.deg, dec * u.deg)


def get_skymap(skyportal, cumulative_probability, event):
    """Build a Skymap for a SkyPortal GCN event.

    Downloads the event's localization from SkyPortal, extracts the MOC at the
    given cumulative_probability threshold, and wraps it with identifying metadata.
    """
    localization = event["localization"]
    bytes_io = skyportal.download_localization(
        localization["dateobs"], localization["localization_name"]
    )
    moc = get_moc_from_fits(bytes_io, cumulative_probability)
    return Skymap(
        dateobs=event["dateobs"],
        alias=next((a for a in event["aliases"] if "#" in a), "No aliases"), # Use the first alias that contains "#"
        moc=moc,
        created_at=localization["created_at"],
        tags=event.get("tags", [])
    )


def get_moc_from_fits(bytes_io, cumulative_probability):
    """Extract MOC from a FITS file containing a HEALPix skymap.

    Parameters
    ----------
    bytes_io : io.BytesIO
        A BytesIO object containing the FITS file data.
    cumulative_probability : float
        The cumulative probability threshold for the MOC.
    """
    with fits.open(bytes_io) as hdul:
        data = hdul[1].data
        columns = [col.name for col in hdul[1].columns]
        header = hdul[1].header

    if "UNIQ" in columns:
        uniq = data["UNIQ"]
        probdensity = data["PROBDENSITY"]
        orders = (np.log2(uniq // 4)) // 2
        area = np.pi / (3 * 4**orders) * u.sr
        prob = probdensity * area
    else:
        prob_col = next(c for c in columns if c in ("PROB", "PROBABILITY", "PROBDENSITY"))
        prob = np.ravel(data[prob_col])
        npix = len(prob)
        nside = int(np.sqrt(npix / 12))
        order = int(np.log2(nside))

        # UNIQ scheme uses NESTED ordering
        ordering = header.get("ORDERING", "NESTED").upper()
        if ordering == "RING":
            ring_hp = HEALPix(nside=nside, order="ring")
            nested_hp = HEALPix(nside=nside, order="nested")
            lon, lat = ring_hp.healpix_to_lonlat(np.arange(npix))
            nested_indices = nested_hp.lonlat_to_healpix(lon, lat)
            reordered = np.empty(npix)
            reordered[nested_indices] = prob
            prob = reordered

        indices = np.arange(npix)
        uniq = 4 * (4 ** order) + indices

    return MOC.from_valued_healpix_cells(uniq, prob, 29, cumul_to=cumulative_probability)


def plot_object_on_skymap(obj, moc):
    """
    Returns a PNG image of the skymap with the object overlaid.

    Parameters
    ----------
    obj : dict
        Object with {"objectId", "ra", "dec"} in degrees.
    moc : MOC
        The MOC object representing the skymap.

    Returns
    -------
    bytes : BytesIO
        A BytesIO object containing the PNG image data.
    """
    projection = WCS({
        "naxis": 2,
        "naxis1": 1620,
        "naxis2": 810,
        "crpix1": 810.5,
        "crpix2": 405.5,
        "cdelt1": -0.2,
        "cdelt2": 0.2,
        "ctype1": "RA---AIT",
        "ctype2": "DEC--AIT",
        "crval1": 0.0,
        "crval2": 0.0,
    })

    fig = plt.figure(figsize=(10, 5))
    ax = fig.add_subplot(1, 1, 1, projection=projection, frame_class=EllipticalFrame)
    moc.fill(ax=ax, wcs=projection, alpha=0.4, color="red")
    moc.border(ax=ax, wcs=projection, color="red")
    ax.grid()
    ax.coords[0].set_ticklabel_visible(False)
    ax.scatter(obj["ra"], obj["dec"], transform=ax.get_transform("world"),marker='*',
               s=120, c="blue", edgecolor="black", label=obj["objectId"], zorder=2)

    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(fig)
    buffer.seek(0)
    return buffer


def display_skymaps(obj, skymaps, plot=False):
    """Display information about the skymaps that match the given object and optionally plot them.

    Parameters
    ----------
    obj : dict
        A dictionary containing the object details, including "objectId", "ra", and "dec".
    skymaps : dict
        A dictionary of skymaps, where the keys are dateobs and the values are Skymap objects.
    plot : bool, optional
        Whether to plot the skymaps using matplotlib. Default is False.
    """
    ra, dec = obj["ra"], obj["dec"]
    log(f"Displaying {len(skymaps)} skymap(s) for {obj['objectId']} (ra={ra:.5f}, dec={dec:.5f}):")
    for dateobs, skymap in skymaps.items():
        is_in = skymap.contains(ra, dec)
        is_match = f"{'  ' if is_in else 'NO'} MATCH"
        log(f"Type: {skymap.type} | Instrument: {skymap.instrument} | Id: {skymap.id} | [{is_match}] {skymap.alias} dateobs={dateobs}")

        if plot:
            fig, ax = plt.subplots(figsize=(10, 5))
            ax.imshow(mpimg.imread(plot_object_on_skymap(obj, skymap.moc)))
            ax.axis("off")
            ax.set_title(f"[{is_match}] {skymap.alias} — {dateobs}")
            plt.show()