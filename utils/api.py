import functools
import io
import time
import requests

from utils.logger import log, RED, YELLOW, ENDC

SLOW_RESPONSE_THRESHOLD = 5  # seconds

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
        response = requests.get(f"{self.base_url}/api/sysinfo", timeout=40)
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
        response = requests.get(
            f"{self.base_url}/api/config",
            headers=self.headers,
            timeout=40
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
            response = requests.request(method, endpoint, params=data, headers=self.headers, timeout=40)
        else:
            response = requests.request(method, endpoint, json=data, headers=self.headers, timeout=40)

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

    def get_gcn_events(self, dateobs):
        """
        Get GCN events from SkyPortal filtered by dateobs and
        specific tags:
        - GW (any size)
        - BNS (any size)
        - NSBH (any size)
        - SVOM (any notice)
        - Einstein Probe (any notice)
        - Fermi (< 1000 sq. deg.)

        Parameters
        ----------
        dateobs : datetime.datetime
            Date of observation to filter GCN events from

        Returns
        -------
        list
            GCN events
        """
        payload = {
            "startDate": dateobs,
            "excludeNoticeContent": True,
        }

        # Get GCN events with GW, BNS, NSBH, SVOM or Einstein Probe and without BBH, MLy or Terrestrial tags.
        gcn_events = self.fetch_all_pages(
            "/api/gcn_event",
            {
                **payload,
                "gcnTagKeep":"GW,BNS,NSBH,SVOM,Einstein Probe",
                "gcnTagRemove": "BBH,MLy,Terrestrial"
            },
            "events"
        )

        # Get GCN events with Fermi tag and localization < 1000 sq.deg.
        gcn_events += self.fetch_all_pages(
            "/api/gcn_event",
            {**payload,"gcnTagKeep": "Fermi","localizationTagKeep": "< 1000 sq. deg."},
            "events"
        )
        return gcn_events

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
            raise ValueError(f"Error fetching localization: {response.text}")
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