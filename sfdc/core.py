"""
Basic implementation of distributed singleflight in python
"""

from functools import partial
from threading import Thread, Lock
import json
from json.decoder import JSONDecodeError
from urllib.parse import urlparse

from waitress import serve
import falcon
import requests
from singleflight.basic import SingleFlight

from sfdc.consistent import Consistent
from sfdc.topology.zk import ZkServiceDiscovery
from sfdc.util.exceptions import SfdcFetchError

def strip_scheme(url):
  # taken from https://stackoverflow.com/questions/21687408/how-to-remove-scheme-from-url-in-python
  parsed = urlparse(url)
  scheme = "%s://" % parsed.scheme
  return parsed.geturl().replace(scheme, '', 1)

class SfdcBackendServer(object):
  def __init__(self, fn):
    self._fn = fn

  def on_post(self, req, resp, key):
    try:
      params = json.load(req.bounded_stream)
    except JSONDecodeError:
      resp.status = falcon.HTTP_BAD_REQUEST
      resp.body = "Bad json data"
      return

    try:
      result = self._fn(key, params, force_this_node=True)
      resp.status = falcon.HTTP_OK
      resp.body = json.dumps(result)
    except Exception as e:
      resp.status = falcon.HTTP_INTERNAL_SERVER_ERROR
      resp.body = str(e)

class SfdcCore(object):
  """
  Coalese/Dedup multiple call with the same coalesce `key`, into one,
  in distributed system setup
  """
  def __init__(
    self, 
    this_host, 
    host_locator, 
    fetching_fn):
    """
    this_host => to check whether locator returning to this_host, if so, 
    this instance will be the one to call `fetching_fn`. Notes, you should pass the scheme too

    host_locator => object that has `locate()` method, may be static, dynamic, or whatever

    fetching_fn => the function to call if this instance is the one to call main resource
    """
    self._this_host = this_host
    self._host_locator = host_locator
    self._sf = SingleFlight()
    self._fn = fetching_fn

    # HTTP/S connection pool
    self._requests = requests.Session()
    http_adapter = requests.adapters.HTTPAdapter(
      pool_connections=10, pool_maxsize=100)
    self._requests.mount('http://', http_adapter)
    https_adapter = requests.adapters.HTTPAdapter(
      pool_connections=10, pool_maxsize=100)
    self._requests.mount('https://', https_adapter)

    # backend Server
    def run_backend_server(api):
      serve(api, listen=strip_scheme(this_host))
    api = falcon.API()
    api.add_route("/{key}", SfdcBackendServer(self.fetch))
    self._backend_server_thread = Thread(
      target=run_backend_server, 
      args=(api,), 
      daemon=True)
    self._backend_server_thread.start()

  def fetch(self, key, params, force_this_node=False):
    """
    Key should be the first parameter

    params is a key-value (map), that will be translated to json,
    if requesting to another server

    returning json map, so design your `fetching_fn` to return as json

    Any error coming from your function, will be directly raised back

    the `force_this_node` parameter is used, to ensure that
    if the service discovery is broken (network-partition, delay update, etc)
    sfdc doesn't fall into loop calling each other until everything is used up
    """
    try:
      url = self._host_locator.locate(key)
    except AttributeError:
      # just for better error message
      raise AttributeError("`host_locator` should implement `locate()` method")

    if force_this_node or (self._this_host == url):
      return self._sf.call(
        self._fn,
        key,
        params=params)

    resp = self._requests.post(f"{url}/{key}", json = json.dumps(params))
    if resp.status_code != 200:
      raise SfdcFetchError(f"Failed to fetch data from {url}")
    return resp.json()
    
def sfdc_consistent_zk(
  zk_client, 
  root_path, 
  this_host, 
  fetching_fn):
  """
  all of these are usually set before anything else
  and on main thread, when bootstrapping

  this function is just for test's simplicity
  """

  c = Consistent(hosts=[this_host])
  zksd = ZkServiceDiscovery(
    zk_client,
    root_path,
    this_host,
    c.reset_with_new)

  return SfdcCore(this_host, c, fetching_fn)
  
