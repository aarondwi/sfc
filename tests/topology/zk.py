import unittest
from functools import partial
from time import sleep

from kazoo.client import KazooClient

from sfc.topology.zk import ZkDiscovery

class TestZkSD(unittest.TestCase):
  def test_main_flow(self):
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
      ZkDiscovery(zc, "/", ch, partial(cb, ch), 1)
      for zc, ch in zip(zk_clients, client_hostnames)
    ]

    # give time for clients to setup, etc
    sleep(2)

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

    for zkc in zk_clients[:-1]:
      zkc.stop()
      
