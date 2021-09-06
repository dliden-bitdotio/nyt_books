"""
    File name: main.py
    Author: Daniel Liden
    Date created: 2021-08-19
    Date last modified: 2021-08-19
    Python Version: 3.9
"""

import requests


def json_from_get_request(url: str, params: dict = None) -> dict:
    """obtains JSON-formatted data accessible with a GET request

    Parameters
    ----------
    url : str
        URL for the extraction endpoint, including any query string.
    params : dict, optional
        Optional parameters for the GET request.
    Returns
    ----------
    dictionary
    """
    r = requests.get(url=url, params=params, timeout=60000)
    return r.json()
