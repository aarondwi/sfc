"""
Basic implementation of distributed singleflight in python
"""

from threading import Thread
import json
from json.decoder import JSONDecodeError
import logging
logger = logging.getLogger(__name__)

import falcon
from singleflight.basic import SingleFlight

from sfc.consistent import Consistent
from sfc.topology.zk import ZkDiscovery
from sfc.util.exceptions import (
  FetchError,
  ListNotValidError
)

class SfcBackendServer(object):
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

class SfcCore(object):
  """
  Coalese/Dedup multiple call with the same coalesce `key`, into one,
  in distributed system setup

  :param this_host: to check whether locator returning to this_host, if so, 
      this instance will be the one to call `fetching_fn`
  :param host_locator: object that has `locate()` method, may be static, dynamic, or whatever
  :param wsgi_serve: a WSGI server, will be passed a `falcon.API()` object
  :param requests: a `requests` module connection pool
  :param fetching_fn: the function to call if this instance is the one to call main resource
  """
  def __init__(
    self,
    this_host,
    host_locator,
    wsgi_serve,
    requests_conn_pool,
    fetching_fn):

    self._this_host = this_host
    self._host_locator = host_locator
    self._sf = SingleFlight()
    self._fn = fetching_fn
    self._requests = requests_conn_pool

    # backend Server
    api = falcon.API()
    api.add_route("/{key}", SfcBackendServer(self.fetch))
    self._backend_server_thread = Thread(
      target=wsgi_serve, 
      args=(api,), 
      daemon=True)
    self._backend_server_thread.start()

  def fetch(self, key, params, force_this_node=False):
    """call `fn` only once, coalesced by key

    :param key: a unique identifier, to coalesce same requests
    :param params: a key-value (map), that will be translated to json,
    if requesting to another server
    :param force_this_node: ensure that
    if the service discovery is broken (network-partition, delay update, etc)
    sfc doesn't fall into loop calling each other until everything is used up

    Any error coming from your function, will be directly raised back

    returning json map, so design your `fetching_fn` to return as json
    """
    if not self._host_locator.still_valid():
      raise ListNotValidError()

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

    logger.info(f"calling key `{key}` on {url}")
    resp = self._requests.post(f"{url}/{key}", json = json.dumps(params))
    if resp.status_code != 200:
      logger.warning("Failed calling {url}, receiving status code {resp.status_code}")
      raise FetchError(f"Failed to fetch data from {url}")
    return resp.json()
