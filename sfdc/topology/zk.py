from sfdc.util.exceptions import StateInvalidException
from kazoo.exceptions import (
    NoNodeError,
    NodeExistsError,
    KazooException
)
from kazoo.client import KazooState
from threading import Thread
from base64 import b64encode, b64decode
from functools import partial
import logging
logging.basicConfig()


class ZkDiscovery(object):
  """
  Zookeeper-based ventually consistent service discovery. This is a variant of Kazoo's `party` recipe

  Does not use anything specific, so can use sequential/gevent based handler easily 
  (just make sure your other codes are using the same)

  By default, it only removes `/zookeeper`, if any, just in case you are using root node. 
  (actually, you shouldn't)

  this implementation does not use lock internally, because kazoo
  guarantee only 1 thread managing the watchers
  """

  def __init__(self, zk_client, root_path, this_host, monitor_cb):
    """Create a service discovery object from Zookeeper
    
    :param zk_client: Kazoo connection to be used
    :param root_path: path to put this consistenthashing's config (use preceding '/')
    :param this host: this instance's hostname, that is also used for (no need for preceding '/')
    :param monitor_cb: callback function, after each hosts change
    """
    self._zk_client = zk_client
    self._root_path = root_path if root_path[-1] == "/" else root_path+"/"
    self._this_host = this_host
    self._monitor_cb = monitor_cb
    self.participating = False

    try:
      self._zk_client.add_listener(self._connection_monitor)
      self._zk_client.start()
      self._zk_client.ensure_path(root_path)
    except AttributeError:
      # just for better error message
      raise AttributeError(
          "Do you pass a proper KazooClient object as zk_client?")

    self.wake_event = zk_client.handler.event_object()
    self.join()
    Thread(target=self.monitor_current_hosts, daemon=True).start()

  def _host_as_b64(self):
    return b64encode(self._this_host.encode('utf8')).decode('utf8')

  def _this_host_full_path(self):
    return f"{self._root_path}{self._host_as_b64()}"

  def join(self):
    return self._zk_client.retry(self._inner_join)

  def _inner_join(self):
    """
    using ephemeral node,
    so if node crashes, should be able to reconnect following the basic flow.
    But if only temporarily disconnected, but still exists, no need to throw error.

    This function assumes no other instance have the same `host` (and it shouldn't actually),
    and is registered in base64 format (so it won't error because of encoding, etc).

    no need to handle other errors, as if this function failed, it is preferable to just crash (no data either way)
    """
    try:
      self._zk_client.create(self._this_host_full_path(), ephemeral=True)
    except NodeExistsError:
      pass

  def monitor_current_hosts(self):
    """
    list all children/nodes, and do watch it

    if the given root node is '/', remove `/zookeeper` from it

    Here, always get all child, cause zk doesnt guarantee no message lost on watchers

    We wait on kazoo `event_object()`, so the watcher doesn't need to always recurse itself,
    at the cost of calling `get_children()` twice
    """
    while True:
      self.wake_event.clear()

      if not self.participating:
        return

      hosts = self._zk_client.retry(
          self._zk_client.get_children, self._root_path)
      if self._root_path == "/":
        hosts.remove("zookeeper")
      hosts = [b64decode(h.encode()).decode() for h in hosts]
      self._monitor_cb(hosts)

      try:
        self._zk_client.retry(
          self._zk_client.get_children,
          self._root_path,
          watch=self._watch_monitor)
      except NoNodeError:
        self.participating = False
        raise StateInvalidException(
            "Base root gone, can't continue monitoring for change")
      except KazooException:
        # mostly will be about disconnected from server
        # meaning we can't safely continue running this instance forever
        # for now, just disable this instance
        self.participating = False
        return
      else:
        self.wake_event.wait()

  def _watch_monitor(self, stat=None):
    self.wake_event.set()

  def _connection_monitor(self, state):
    self.participating = state != KazooState.LOST
    if not self.participating:
      self.wake_event.set()

  def still_valid(self):
    """
    returns whether this instance still valid (still connected, can manage its quorum via zk)
    """
    return self.participating

  def stop(self, stop_zk_client=False):
    self.participating = False
    result = self._zk_client.retry(self._inner_stop)
    if stop_zk_client:
      self._zk_client.stop()
    return result

  def _inner_stop(self):
    try:
      self._zk_client.delete(self._this_host_full_path())
    except NoNodeError:
      return False
    return True
