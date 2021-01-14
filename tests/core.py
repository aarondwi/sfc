import unittest
from functools import partial
from time import sleep
from threading import Thread, Lock
from socket import gethostname
from urllib.parse import urlparse

from kazoo.client import KazooClient
import requests
import waitress

from sfc.core import SfcCore
from sfc.consistent import Consistent
from sfc.topology.zk import ZkDiscovery

class ZkConsistent(object):
  def __init__(
    self,
    zk_client, 
    root_path, 
    this_host,
    jitter_range=1,
    disconnected_timeout=60):

    self._consistent = Consistent(hosts=[this_host])
    self._zksd = ZkDiscovery(
      zk_client,
      root_path,
      this_host,
      self._consistent.reset_with_new,
      jitter_range,
      disconnected_timeout)

  def locate(self, key):
    return self._consistent.locate(key)

  def still_valid(self):
    return self._zksd.still_valid()

def create_sfc_consistent_zk(
  zk_client, 
  root_path, 
  this_host, 
  fetching_fn):
  """
  all of these are usually set before anything else
  and on main thread, when bootstrapping

  this function is just for test's simplicity
  """

  def strip_scheme(url):
    # taken from https://stackoverflow.com/questions/21687408/how-to-remove-scheme-from-url-in-python
    parsed = urlparse(url)
    scheme = "%s://" % parsed.scheme
    return parsed.geturl().replace(scheme, '', 1)

  wsgi_serve = partial(waitress.serve, listen=strip_scheme(this_host))

  requests_conn_pool = requests.Session()
  http_adapter = requests.adapters.HTTPAdapter(
    pool_connections=10, pool_maxsize=100)
  requests_conn_pool.mount('http://', http_adapter)

  zkc = ZkConsistent(zk_client, root_path, this_host)
  return SfcCore(this_host, zkc, wsgi_serve, requests_conn_pool, fetching_fn)

class TestSfcCore(unittest.TestCase):
  def test_singlecall_over_network(self):
    print(f"Running test: `test_singlecall_over_network`")

    hosts = [
      f"http://{gethostname()}:7001", 
      f"http://{gethostname()}:7002", 
      f"http://{gethostname()}:7003"]
    zk_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
    zk_clients = [
      KazooClient(hosts = zk_hosts) 
      for i in range(len(hosts))]

    cb_counter = 0
    def cb(host, params): 
      nonlocal cb_counter
      # emulate latency, so can coalesce
      # only 1 will reach this
      sleep(2)
      cb_counter += params['val']
      return {"status": "OK", "host": host}

    sc = []
    for host, zc in zip(hosts, zk_clients):
      sc.append(
        create_sfc_consistent_zk(
          zk_client=zc,
          root_path="/",
          this_host=host,
          fetching_fn=partial(cb, host)
      ))

    # give time for clients to setup
    sleep(2)

    key = "test-key-for-unit-testing"
    params = {"val": 1}

    # setup our own consistent
    # so we can know where it fell to
    c = Consistent(hosts=hosts)
    result_url = c.locate(key)

    def working_thread(s, key, params):
      nonlocal result_url
      resp = s.fetch(key, params)
      self.assertEqual(resp['status'], "OK")
      self.assertEqual(resp['host'], result_url)

    ts = []
    for s in sc:
      t = Thread(
        target=working_thread, 
        args=(s, key, params,))
      t.start()
      ts.append(t)

    for t in ts:
      t.join()
    self.assertEqual(cb_counter, 1)

    for zkc in zk_clients:
      zkc.stop()

  def test_singlecall_force_this_node(self):
    print(f"Running test: `test_singlecall_force_this_node`")

    hosts = [
      f"http://{gethostname()}:8001", 
      f"http://{gethostname()}:8002", 
      f"http://{gethostname()}:8003"]
    zk_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
    zk_clients = [
      KazooClient(hosts = zk_hosts) 
      for i in range(len(hosts))]

    cb_counter = 0
    lock = Lock()
    def cb(host, params): 
      nonlocal cb_counter, lock
      with lock:
        cb_counter += params['val']
      return {"status": "OK", "host": host}

    sc = []
    for host, zc in zip(hosts, zk_clients):
      sc.append(
        create_sfc_consistent_zk(
          zk_client=zc,
          root_path="/",
          this_host=host,
          fetching_fn=partial(cb, host)
      ))

    # give time for clients to setup
    sleep(3)

    key = "test-key-for-unit-testing-force-this-node"
    params = {"val": 1}

    def working_thread(s, key, params):
      resp = s.fetch(key, params, force_this_node=True)
      self.assertEqual(resp['status'], "OK")

    ts = []
    for s in sc:
      t = Thread(
        target=working_thread, 
        args=(s, key, params,))
      t.start()
      ts.append(t)

    for t in ts:
      t.join()
    self.assertEqual(cb_counter, 3)

    for zkc in zk_clients:
      zkc.stop()
    