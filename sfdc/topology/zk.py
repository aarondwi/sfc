import logging
logging.basicConfig()

from kazoo.client import KazooState
from kazoo.exceptions import NodeExistsError, NoNodeError

class ZkServiceDiscovery(object):
  """
  Eventually consistent service discovery, using kazoo

  Does not use anything specific, so can use sequential/gevent based handler easily 
  (just make sure your other codes are using the same)

  This implementation does not assume where you use which node as its root, 
  so this implementation only removes `/zookeeper`, if any. (And neither should you use `/zookeeper` as name of any znode)
  """
  def __init__(self, zk_client, this_host, monitor_cb):
    """
    zk_client => Kazoo connection to be used

    this host => host value, that is also used for

    monitor_cb => callback function, after each hosts change
    """
    self._zk_client = zk_client
    self._this_host = this_host
    self._monitor_cb = monitor_cb

    self.register(self._this_host)
    self.monitor_current_hosts()

  def register(self, host):
    """
    using ephemeral node,
    so if node crashes, should be able to reconnect following the basic flow.
    But if only temporarily disconnected, but still exists, no need to throw error.

    This function assumes no other instance have the same `host` (and it shouldn't actually)

    no need to handle other errors, as if this function failed, it is preferable to just crash (no data either way)
    """
    self._zk_client.create(host, ephemeral = True)

  def monitor_current_hosts(self, stat = None):
    """
    list all children/nodes, and do watch it

    if the given root node is '/', remove `/zookeeper` from it
    """
    hosts = self._zk_client.get_children(
      "/", 
      watch = self.monitor_current_hosts)
    hosts.remove("zookeeper")
    self._monitor_cb(hosts)
