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
class SkymapDistance:
    """Per-pixel distance ansatz of a 3D GW skymap: p(r) is proportional to r^2 N(r; distmu, distsigma)."""
    uniq: np.ndarray
    distmu: np.ndarray
    distsigma: np.ndarray
    _lookup: dict = field(init=False, repr=False)

    def __post_init__(self):
        levels, ipix = uniq_to_level_ipix(self.uniq)
        self._lookup = {}
        for level in np.unique(levels):
            selected = np.nonzero(levels == level)[0]
            self._lookup[int(level)] = dict(zip(ipix[selected].tolist(), selected.tolist()))

    def at(self, ra, dec):
        """Return (distmu, distsigma) in Mpc at the given position, or None outside the map."""
        for level, pixels in self._lookup.items():
            ipix = HEALPix(nside=2**level, order="nested").lonlat_to_healpix(ra * u.deg, dec * u.deg)
            index = pixels.get(int(ipix))
            if index is not None:
                mu, sigma = float(self.distmu[index]), float(self.distsigma[index])
                if np.isfinite(mu) and np.isfinite(sigma) and sigma > 0:
                    return mu, sigma
                return None
        return None


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
    area_90 : float or None
        Area of the 90% credible region, in square degrees.
    distance : SkymapDistance or None
        Per-pixel distance of the localization, for 3D GW skymaps only.
    jd : float
        The Julian Date corresponding to dateobs.
    """
    dateobs: str
    alias: str
    moc: MOC
    created_at: str
    tags: list[str]
    area_90: float = None
    distance: SkymapDistance = None
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


def get_alias(event):
    """Return the "instrument#id" alias of a SkyPortal GCN event, or None if it has none.

    Swift events have no such alias in SkyPortal, so it is built from their trigger_id.
    """
    alias = next((a for a in event.get("aliases") or [] if "#" in a), None)
    if alias:
        return alias
    if event.get("trigger_id") and "SWIFT" in (event.get("tags") or []):
        return f"SWIFT#{event['trigger_id']}"
    return None


def get_skymap(skyportal, cumulative_probability, event):
    """Build a Skymap for a SkyPortal GCN event.

    Downloads the event's localization from SkyPortal, extracts the MOC at the
    given cumulative_probability threshold, and wraps it with identifying metadata.
    """
    localization = event["localization"]
    bytes_io = skyportal.download_localization(
        localization["dateobs"], localization["localization_name"]
    )
    moc, area_90, distance = read_skymap_fits(bytes_io, cumulative_probability)
    return Skymap(
        dateobs=event["dateobs"],
        alias=get_alias(event) or "No aliases",
        moc=moc,
        created_at=localization["created_at"],
        tags=event.get("tags", []),
        area_90=area_90,
        distance=distance,
    )


def uniq_to_level_ipix(uniq):
    """Split HEALPix UNIQ indices into their level (log2 nside) and NESTED pixel index."""
    uniq = np.asarray(uniq, dtype=np.int64)
    levels = (np.log2(uniq // 4) // 2).astype(np.int64)
    return levels, uniq - 4 * 4**levels


def read_skymap_fits(bytes_io, cumulative_probability):
    """Read a FITS HEALPix skymap.

    Parameters
    ----------
    bytes_io : io.BytesIO
        A BytesIO object containing the FITS file data.
    cumulative_probability : float
        The cumulative probability threshold for the MOC.

    Returns
    -------
    tuple
        The MOC at cumulative_probability, the 90% area in square degrees,
        and the SkymapDistance (None if the skymap has no distance columns).
    """
    with fits.open(bytes_io) as hdul:
        data = hdul[1].data
        columns = [col.name for col in hdul[1].columns]
        header = hdul[1].header

    distance_columns = {name: None for name in ("DISTMU", "DISTSIGMA")}
    if "UNIQ" in columns:
        uniq = np.asarray(data["UNIQ"], dtype=np.int64)
        prob = np.asarray(data["PROBDENSITY"]) * np.pi / (3 * 4.0**uniq_to_level_ipix(uniq)[0])
        for name in distance_columns:
            if name in columns:
                distance_columns[name] = np.asarray(data[name], dtype=float)
    else:
        prob_col = next(c for c in columns if c in ("PROB", "PROBABILITY", "PROBDENSITY"))
        prob = np.ravel(data[prob_col])
        npix = len(prob)
        nside = int(np.sqrt(npix / 12))
        order = int(np.log2(nside))

        # UNIQ scheme uses NESTED ordering
        nested_indices = np.arange(npix)
        if header.get("ORDERING", "NESTED").upper() == "RING":
            lon, lat = HEALPix(nside=nside, order="ring").healpix_to_lonlat(np.arange(npix))
            nested_indices = HEALPix(nside=nside, order="nested").lonlat_to_healpix(lon, lat)

        def to_nested(values):
            reordered = np.empty(npix)
            reordered[nested_indices] = np.ravel(values)
            return reordered

        prob = to_nested(prob)
        for name in distance_columns:
            if name in columns:
                distance_columns[name] = to_nested(data[name])
        uniq = 4 * (4 ** order) + np.arange(npix)

    pixel_area_deg2 = np.pi / (3 * 4.0**uniq_to_level_ipix(uniq)[0]) * (180 / np.pi) ** 2
    order_by_density = np.argsort(-prob / pixel_area_deg2)
    in_90 = order_by_density[: np.searchsorted(np.cumsum(prob[order_by_density]), 0.9) + 1]
    area_90 = float(pixel_area_deg2[in_90].sum())

    distance = None
    if all(values is not None for values in distance_columns.values()):
        distance = SkymapDistance(uniq=uniq, distmu=distance_columns["DISTMU"], distsigma=distance_columns["DISTSIGMA"])

    moc = MOC.from_valued_healpix_cells(uniq, prob, 29, cumul_to=cumulative_probability)
    return moc, area_90, distance


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