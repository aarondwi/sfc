import unittest
from functools import partial
from time import sleep

from kazoo.client import KazooClient

from sfdc.topology.zk import ZkServiceDiscovery

class TestZkSD(unittest.TestCase):
  def test_main_flow(self):
    zk_hosts = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'

    number_of_clients = 3

    zk_clients = [
      KazooClient(hosts = zk_hosts) 
      for i in range(number_of_clients)]
    for zkc in zk_clients:
      zkc.start()
    
    client_hostnames = [
      '127.0.0.1:700' + str(i)
      for i in range(number_of_clients)]
    client_map = {
      ch: 0
      for ch in client_hostnames}

    def cb(client_hostname, hosts):
      client_map[client_hostname] = len(hosts)

    ZkSDs = [
      ZkServiceDiscovery(
        zk_clients[i],
        client_hostnames[i],
        partial(cb, client_hostnames[i]))
      for i in range(number_of_clients)
    ]

    # give time for clients to setup, etc
    sleep(2)

    for ch in client_hostnames:
      self.assertEqual(
        client_map[ch], 
        len(client_hostnames)
      )

    # disable one of them
    zk_clients[len(zk_clients)-1].stop()

    # give time for changes to propagate
    sleep(2)

    for ch in client_hostnames[:-1]:
      self.assertEqual(
        client_map[ch], 
        len(client_hostnames) - 1
      )

    for zkc in zk_clients[:-1]:
      zkc.stop()
      