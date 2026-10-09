import functools
import io
import time
import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.logger import log, RED, YELLOW, ENDC

SLOW_RESPONSE_THRESHOLD = 5  # seconds
REQUEST_TIMEOUT = 10  # seconds

class APIError(Exception):
    pass


def handle_timeout(method):
    """
    Decorator to handle requests timeouts and log slow responses.
    If a request takes longer than 5 seconds, log a warning.
    If a request times out, raise a TimeoutError with a custom message.
    """
    def get_request_type(method_name, args):
        """Return the method name or endpoint being called if method is 'api'"""
        if method_name == "api" and len(args) > 1:
            return args[1] # endpoint
        return method_name

    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        try:
            start = time.time()
            result = method(self, *args, **kwargs)

            latency = time.time() - start
            if latency > SLOW_RESPONSE_THRESHOLD:
                log(f"{YELLOW}Warning - SkyPortal API is responding slowly to {get_request_type(method.__name__, args)} requests: {latency:.2f} seconds{ENDC}")

            return result
        except APIError as e:
            raise APIError(f"{RED}Api error in {get_request_type(method.__name__, args)}{ENDC} - {e}")
        except requests.exceptions.Timeout:
            raise APIError(f"{RED}Timeout error{ENDC} - SkyPortal API not responding to {YELLOW}{get_request_type(method.__name__, args)}{ENDC} request")
        except requests.exceptions.RequestException as e:
            raise APIError(f"{RED}Request error{ENDC} in {get_request_type(method.__name__, args)} - {type(e).__name__}")
    return wrapper


class SkyPortal:
    """
    SkyPortal API client

    Parameters
    ----------
    instance : str
        Base URL of the SkyPortal instance (e.g. https://fritz.science)
    port : int
        Port to use
    token : str
        SkyPortal API token
    validate : bool, optional
        If True, validate the SkyPortal instance and token
    
    Attributes
    ----------
    base_url : str
        Base URL of the SkyPortal instance
    headers : dict
        Authorization headers to use
    """

    def __init__(self, instance, token, port=443, validate=True):
        # build the base URL from the instance and port
        self.base_url = instance
        if port and port not in (80, 443):
            self.base_url += f':{port}'
        
        self.headers = {'Authorization': f'token {token}'}
        self.ns_probabilities = {}  # {(dateobs, number of notices): P(BNS) + P(NSBH)}

        self.session = requests.Session()
        adapter = HTTPAdapter(max_retries=Retry(
            total=2, backoff_factor=1, status_forcelist=[500, 502, 503, 504], raise_on_status=False
        ))
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        # ping it to make sure it's up, if validate is True
        if validate:
            if not self.ping():
                raise ValueError('SkyPortal API not available')
            
            if not self.auth():
                raise ValueError('SkyPortal API authentication failed. Token may be invalid.')

    @handle_timeout
    def ping(self):
        """
        Check if the SkyPortal API is available

        Returns
        -------
        bool
            True if the API is available, False otherwise
        """
        response = self.session.get(f"{self.base_url}/api/sysinfo", timeout=REQUEST_TIMEOUT)
        return response.status_code == 200

    @handle_timeout
    def auth(self):
        """
        Check if the SkyPortal Token provided is valid

        Returns
        -------
        bool
            True if the token is valid, False otherwise
        """
        response = self.session.get(
            f"{self.base_url}/api/config",
            headers=self.headers,
            timeout=REQUEST_TIMEOUT
        )
        return response.status_code == 200

    @handle_timeout
    def api(self, method: str, endpoint: str, data=None, return_response=False):
        """
        Make an API request to SkyPortal

        Parameters
        ----------
        method : str
            HTTP method to use (GET, POST, PUT, PATCH, DELETE)
        endpoint : str
            API endpoint to query
        data : dict, optional
            JSON data to send with the request, as parameters or payload
        return_response : bool, optional
            If True, return the raw response instead of parsing JSON

        Returns
        -------
        requests.Response or dict
            If `return_response` is True, returns the raw `requests.Response` object.
            Otherwise, returns the parsed JSON response as a dictionary.
        """
        endpoint = f'{self.base_url}/{endpoint.strip("/")}'
        if method == 'GET':
            response = self.session.request(method, endpoint, params=data, headers=self.headers, timeout=REQUEST_TIMEOUT)
        else:
            response = self.session.request(method, endpoint, json=data, headers=self.headers, timeout=REQUEST_TIMEOUT)

        if return_response:
            return response

        try:
            body = response.json()
        except Exception:
            raise APIError("Server error." if "server error" in response.text.lower() else response.text)

        if response.status_code != 200:
            raise APIError(body.get("message", response.text))

        return body.get('data')

    def fetch_all_pages(self, endpoint, payload, item_key):
        """
        Fetch all pages of a paginated API endpoint

        Returns
        -------
        list
            All items from all pages
        """
        items = []
        payload["pageNumber"] = 1
        payload["numPerPage"] = 1000
        while True:
            results = self.api("GET", endpoint, data=payload)
            items += results[item_key]
            if results["totalMatches"] <= len(items):
                break
            payload["pageNumber"] += 1
            time.sleep(0.3)
        return items

    def get_ns_probability(self, event):
        """
        P(BNS) + P(NSBH) of a GW event, from the properties of its most recent notice

        Properties are only fetched again when the event receives a new notice.

        Parameters
        ----------
        event : dict
            GCN event from the /api/gcn_event list

        Returns
        -------
        float
            Probability that the event is an astrophysical merger with a neutron star, 0 if unknown
        """
        key = (event["dateobs"], len(event.get("gcn_notices") or []))
        if key not in self.ns_probabilities:
            details = self.api("GET", f"/api/gcn_event/{event['dateobs']}", data={"excludeNoticeContent": True})
            latest = next(
                (p["data"] for p in details.get("properties") or [] if "BNS" in p["data"] or "NSBH" in p["data"]),
                {}
            )
            self.ns_probabilities = {k: v for k, v in self.ns_probabilities.items() if k[0] != event["dateobs"]}
            self.ns_probabilities[key] = (latest.get("BNS") or 0) + (latest.get("NSBH") or 0)
        return self.ns_probabilities[key]

    def get_gcn_events(self, dateobs, ns_probability_threshold=0.5, include_gw=True):
        """
        Get GCN events from SkyPortal filtered by dateobs:
        - GW with P(BNS) + P(NSBH) >= ns_probability_threshold (any size, not retracted, not MLy)
        - SVOM (any notice)
        - Einstein Probe (any notice)
        - Fermi (< 1000 sq. deg.)
        - Swift GRB (< 1000 sq. deg.)

        Parameters
        ----------
        dateobs : datetime.datetime
            Date of observation to filter GCN events from
        ns_probability_threshold : float, optional
            Minimum P(BNS) + P(NSBH) of the most recent notice for a GW event to be kept
        include_gw : bool, optional
            If False, skip the GW events (when they are received directly from GCN)

        Returns
        -------
        list
            GCN events, deduplicated by dateobs
        """
        payload = {
            "startDate": dateobs,
            "excludeNoticeContent": True,
        }

        gcn_events = []
        if include_gw:
            gw_events = self.fetch_all_pages(
                "/api/gcn_event",
                {**payload, "gcnTagKeep": "GW", "gcnTagRemove": "retracted,MLy"},
                "events"
            )
            gcn_events = [event for event in gw_events if self.get_ns_probability(event) >= ns_probability_threshold]
            gw_dateobs = {event["dateobs"] for event in gw_events}
            self.ns_probabilities = {k: v for k, v in self.ns_probabilities.items() if k[0] in gw_dateobs}

        gcn_events += self.fetch_all_pages(
            "/api/gcn_event",
            {**payload, "gcnTagKeep": "SVOM,Einstein Probe"},
            "events"
        )

        gcn_events += self.fetch_all_pages(
            "/api/gcn_event",
            {**payload, "gcnTagKeep": "Fermi", "localizationTagKeep": "< 1000 sq. deg."},
            "events"
        )

        gcn_events += self.fetch_all_pages(
            "/api/gcn_event",
            {**payload, "gcnTagKeep": "SWIFT", "gcnTagRemove": "Not GRB", "localizationTagKeep": "< 1000 sq. deg."},
            "events"
        )

        return list({event["dateobs"]: event for event in gcn_events}.values())

    def download_localization(self, dateobs, localization_name):
        """
        Download localization as a FITS file from SkyPortal.

        Returns
        -------
        io.BytesIO
            A BytesIO object containing the FITS file data.
        """
        response = self.api(
            "GET",
            f"/api/localization/{dateobs}/name/{localization_name}/download",
            return_response=True
        )
        if response.status_code != 200:
            raise ValueError(f"Error fetching localization: HTTP {response.status_code} {response.reason}")
        return io.BytesIO(response.content) # return a BytesIO object containing the FITS file

    def get_objects(self, payload):
        """
        Get objects from SkyPortal

        Parameters
        ----------
        payload : dict
            Dictionary of parameters to send with the request

        Returns
        -------
        list
            Objects
        """
        return self.fetch_all_pages("/api/candidates", payload, "candidates")

    def get_object_photometry(self, obj_id):
        """
        Get photometry for a specific object from SkyPortal

        Parameters
        ----------
        obj_id : str
            ID of the object to get photometry for

        Returns
        -------
        list
            Photometry data
        """
        payload = {
            "individualOrSeries": "individual",
            "deduplicatePhotometry": True
        }
        return self.api("GET", f"/api/sources/{obj_id}/photometry", payload)

    def get_instruments(self):
        """
        Get instruments from SkyPortal

        Returns
        -------
        list
            Instruments
        """
        return self.api("GET", "/api/instrument")