import logging
logging.basicConfig()
from functools import partial

from kazoo.client import KazooState
from kazoo.exceptions import (
  NoNodeError,
  ConnectionClosedError,
  SessionExpiredError,
  ConnectionLoss,
  ConnectionDropped
)

from sfdc.util.exceptions import StateInvalidException

class ZkServiceDiscovery(object):
  """
  Eventually consistent service discovery, using kazoo

  Does not use anything specific, so can use sequential/gevent based handler easily 
  (just make sure your other codes are using the same)

  Do NOT start the KazooClient passed to this implementation,
  because it needs to add listener (to monitor connection)

  By default, it only removes `/zookeeper`, if any, just in case you are using root node. 
  (actually, you shouldn't)

  this implementation does not use lock internally, because kazoo
  guarantee only 1 thread managing the watchers
  """
  def __init__(self, zk_client, root_path, this_host, monitor_cb):
    """
    zk_client => Kazoo connection to be used

    root_path => path to put this consistenthashing's config (use preceding '/')

    this host => host value, that is also used for (no need for preceding '/')

    monitor_cb => callback function, after each hosts change
    """
    self._zk_client = zk_client
    self._root_path = root_path
    self._this_host = this_host
    self._monitor_cb = monitor_cb
    self._is_valid = None

    self._zk_client.add_listener(self._connection_monitor)
    self._zk_client.start()

    self._zk_client.ensure_path(root_path)
    self.register()
    self.monitor_current_hosts()

  def register(self):
    """
    using ephemeral node,
    so if node crashes, should be able to reconnect following the basic flow.
    But if only temporarily disconnected, but still exists, no need to throw error.

    This function assumes no other instance have the same `host` (and it shouldn't actually)

    no need to handle other errors, as if this function failed, it is preferable to just crash (no data either way)
    """
    self._zk_client.create(
      f"{self._root_path}/{self._this_host}", 
      ephemeral = True)

  def monitor_current_hosts(self, stat = None):
    """
    list all children/nodes, and do watch it

    if the given root node is '/', remove `/zookeeper` from it

    Here, always get all child, cause zk doesnt guarantee no message lost on watchers
    """
    try:
      hosts = self._zk_client.get_children(
        self._root_path,
        watch = self.monitor_current_hosts)
    except NoNodeError:
      raise StateInvalidException("Base root gone, can't continue monitoring for change")
    except (
      ConnectionClosedError, 
      SessionExpiredError,
      ConnectionLoss,
      ConnectionDropped):
      self._is_valid = False

      # for now, just disable this instance
      return
    
    if self._root_path == "/":
      hosts.remove("zookeeper")
    self._monitor_cb(hosts)

  def _connection_monitor(self, state):
    self._is_valid = state == KazooState.CONNECTED

  def still_valid(self):
    """
    returns whether this instance still valid (still connected, can manage its quorum via zk)
    """
    return self._is_valid
