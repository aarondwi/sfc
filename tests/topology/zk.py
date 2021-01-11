import unittest
from functools import partial
from time import sleep
from base64 import b64encode

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

from sfc.topology.zk import ZkDiscovery

class TestZkSD(unittest.TestCase):
  def test_main_flow(self):
    print("\ntest_main_flow\n")

    zk_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
    number_of_clients = 3

    zk_clients = [
      KazooClient(hosts = zk_hosts) 
      for i in range(number_of_clients)]
    
    client_hostnames = [
      '127.0.0.1:700' + str(i)
      for i in range(number_of_clients)]
    client_map = {
      ch: 0
      for ch in client_hostnames}

    def cb(client_hostname, hosts):
      client_map[client_hostname] = len(hosts)

    ZkSDs = [
      ZkDiscovery(zc, "/sfc", ch, partial(cb, ch),1)
      for zc, ch in zip(zk_clients, client_hostnames)
    ]

    # give time for clients to setup, etc
    sleep(3)

    for ch in client_hostnames:
      self.assertEqual(
        client_map[ch], 
        len(client_hostnames)
      )

    for zkc in ZkSDs:
      self.assertTrue(zkc.still_valid())

    # disable one of them
    # it should be invalid directly
    # zk_clients[len(zk_clients)-1].stop()
    ZkSDs[-1].stop(stop_zk_client=True)
    self.assertFalse(ZkSDs[-1].still_valid())

    # give time for changes to propagate
    sleep(2)

    for ch in client_hostnames[:-1]:
      self.assertEqual(
        client_map[ch], 
        len(client_hostnames) - 1
      )

    # cleanup
    for zksd in ZkSDs[:-1]:
      zkc.stop(stop_zk_client=False)
    
    zk_clients[0].delete("/sfc", recursive=True)
    for zk in zk_clients[:-1]:
      zk.stop()

    sleep(1)
      
  def test_node_exist_then_conn_lost(self):
    print("\ntest_error_handle\n")

    zk_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
    zk_client = KazooClient(hosts = zk_hosts)
    zk_client.start()
    client_hostname = "127.0.0.1:7001"

    path_exist = f"/sfc/{b64encode(client_hostname.encode('utf8')).decode('utf8')}"
    try:
      zk_client.create("/sfc")
    except NodeExistsError:
      pass
    zk_client.create(path_exist, ephemeral=True)

    def cb(hosts):
      pass

    zk_client_2 = KazooClient(hosts = zk_hosts)
    ZkSD = ZkDiscovery(zk_client_2, "/sfc", client_hostname, cb, 1, 1)

    # wait for ZkSD to prepare
    # and here, should not crash because node already exist
    sleep(2)

    # wait for `_monitor_kill_instance` to kick off
    # we stop the zk_client, so our ZkSD still think it is valid, until it is set otherwise
    zk_client_2.stop()
    sleep(2)
    self.assertFalse(ZkSD.still_valid())

    # cleanup
    ZkSD.stop(stop_zk_client=False)
    zk_client.delete("/sfc", recursive=True)
    zk_client.stop()
    sleep(1)

  def test_param_check(self):
    print("\ntest_param_check\n")

    zk_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
    zk_client = KazooClient(hosts = zk_hosts)
    zk_client.start()
    client_hostname = "127.0.0.1:7001"

    def cb(hosts):
      pass

    zk_client_2 = KazooClient(hosts = zk_hosts)
    ZkSD = ZkDiscovery(zk_client_2, "/sfc", client_hostname, cb, -1, -1)

    self.assertEqual(ZkSD._jitter_range, 5)
    self.assertEqual(ZkSD._disconnected_timeout, 60)

  def test_node_no_longer_exist(self):
    print("\ntest_node_no_longer_exist\n")

    zk_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
    zk_client = KazooClient(hosts = zk_hosts)
    zk_client.start()
    client_hostname = "127.0.0.1:7001"

    def cb(hosts):
      pass

    zk_client_2 = KazooClient(hosts = zk_hosts)
    ZkSD = ZkDiscovery(zk_client_2, "/sfc", client_hostname, cb, 1, 1)

    zk_client.delete("/sfc", recursive=True)
    sleep(2)

    self.assertFalse(ZkSD.still_valid())
