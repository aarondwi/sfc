import unittest
from functools import partial
from time import sleep
from threading import Thread, Lock
from socket import gethostname

from kazoo.client import KazooClient

from sfdc.core import sfdc_consistent_zk
from sfdc.consistent import Consistent

class TestSfdcCore(unittest.TestCase):
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
        sfdc_consistent_zk(
          zk_client=zc,
          root_path="/",
          this_host=host,
          fetching_fn=partial(cb, host)
      ))

    # give time for clients to setup
    sleep(3)

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

    [t.join() for t in ts]
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
        sfdc_consistent_zk(
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

    [t.join() for t in ts]
    self.assertEqual(cb_counter, 3)

    for zkc in zk_clients:
      zkc.stop()
    