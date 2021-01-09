from threading import Thread
from base64 import b64encode, b64decode
from functools import partial
import random
from time import sleep
import logging
logger = logging.getLogger(__name__)

from kazoo.exceptions import (
    NoNodeError,
    NodeExistsError,
    KazooException
)
from kazoo.client import KazooState

from sfc.util.exceptions import (
  StateInvalidException,
  DisconnectedTooLongError
)

class ZkDiscovery(object):
  """
  Zookeeper-based eventually consistent service discovery. This is a variant of Kazoo's `party` recipe

  Does not use anything specific, so can use sequential/gevent based handler easily 
  (just make sure your other codes are using the same)

  By default, it only removes `/zookeeper`, if any, just in case you are using root node. 
  (actually, you shouldn't)

  this implementation does not use lock internally, as kazoo
  guarantee only 1 thread managing the watchers, doing so from 1 queue

  :param zk_client: Kazoo connection to be used
  :param root_path: path to put this consistenthashing's config (use preceding '/')
  :param this host: this instance's hostname, that is also used for (no need for preceding '/')
  :param monitor_cb: callback function, after each hosts change
  :param jitter_range: do call zk after some jitter sleep, to reduce herd
  :param disconnected_timeout: timeout to kill this instance after specified timeout, to prevent it from drifting even further
  """

  def __init__(self, zk_client, root_path, this_host, monitor_cb, jitter_range, disconnected_timeout=60):
    
    self._zk_client = zk_client
    self._root_path = root_path if root_path[-1] == "/" else root_path+"/"
    self._this_host = this_host
    self._monitor_cb = monitor_cb
    self._disconnected_timeout = disconnected_timeout

    self._jitter_range = jitter_range
    if self._jitter_range and self._jitter_range <= 0:
      # allow negative param, but reset it to default value
      self._jitter_range = 5
    
    self.previous_state = KazooState.LOST
    self.participating = False
    self.participation_lock = zk_client.handler.lock_object()
    self.host_change_event = zk_client.handler.event_object()
    self.current_register_state = False
    self.currently_watch_disconnect = False

    try:
      self._zk_client.add_listener(self._connection_monitor)
      self._zk_client.start()
      self._zk_client.ensure_path(root_path)
    except AttributeError:
      # just for better error message
      raise AttributeError(
          "Do you pass a proper KazooClient object as zk_client?")
        
    Thread(target=self._monitor_current_hosts, daemon=True).start()

  def _host_as_b64(self):
    return b64encode(self._this_host.encode('utf8')).decode('utf8')

  def _this_host_full_path(self):
    return f"{self._root_path}{self._host_as_b64()}"

  def _monitor_join(self):
    # after join returns, we can be sure that it succeeds
    result = self._join()
    if result:
      with self.participation_lock:
        self.participating = True
        self.current_register_state = False

  def _join(self):
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
      return True
    except NodeExistsError:
      logger.warn(
        "Node exists error found,"
        "meaning some interruption to network/node happened."
        "Check if any of your apps are affected")
      return True
    except KazooException:
      return False

  def _monitor_current_hosts(self):
    """
    list all children/nodes, and do watch it

    if the given root node is '/', remove `/zookeeper` from it

    Here, always get all child, cause zk doesnt guarantee no message lost on watchers
    """
    while True:
      # we cover our monitoring on loop, and delay with sleep
      # because when disconnected, we dont want to continue trying calling zk too much

      while self.participating:
        self.host_change_event.clear()

        # we sleep for a bit, for random duration
        # at most self._jitter_range
        #
        # this is done to prevent herd call after each membership change
        self._jittered_sleep()

        try:
          hosts = self._zk_client.retry(
            self._zk_client.get_children,
            self._root_path,
            watch=self._watch_monitor)

          if self._root_path == "/":
            hosts.remove("zookeeper")
          hosts = [b64decode(h.encode()).decode() for h in hosts]
          self._monitor_cb(hosts)

        except NoNodeError:
          with participation_lock:
            self.participating = False
          logger.error("Base root gone, can't continue monitoring for change", exc_info=1)
          raise StateInvalidException(
              "Base root gone, can't continue monitoring for change")
        except KazooException:
          # Kazoo exposes connection watcher
          # which does check for this kind of error (mostly connection)
          # we will only silent it, 
          # and handle all in the connection watcher (`self._connection_monitor`)
          logger.error("Found un-handled kazoo exception:", exc_info=1)
        
        self.host_change_event.wait()
      
      # wait a bit before polling self.participating again
      sleep(1)

  def _watch_monitor(self, stat=None):
    self.host_change_event.set()

  def _connection_monitor(self, state):
    with self.participation_lock:
      if (state == KazooState.CONNECTED 
        and self.previous_state == KazooState.LOST
        and not self.current_register_state):
        logger.info(f"{self._this_host} trying to join right now")
        # because our membership use ephemeral node,
        # after we reinstated our connection,
        # we need to re-declared our instance to the membership
        #
        # self.current_register_state is a barrier
        # to ensure only one thread to join is running
        #
        # we do not change the participating flag here
        # because need to successfully register first before re-running
        self.current_register_state = True
        Thread(target=self._monitor_join, daemon=True).start()
    
      if state != KazooState.CONNECTED:
        self.participating = False
        if not not self.currently_watch_disconnect:
          self.currently_watch_disconnect = True
          Thread(target=self._monitor_kill_instance, daemon=True).start()
        logger.info(f"{self._this_host} is currently disconnected")

      self.previous_state = state

  def still_valid(self):
    """
    returns whether this instance still valid (still connected, can manage its quorum via zk)
    """
    return self.participating

  def stop(self, stop_zk_client=False):
    with self.participation_lock:
      self.participating = False

    # best effort stopping zk_client
    self._zk_client.retry(self._inner_stop)
    if stop_zk_client:
      self._zk_client.stop()

  def _inner_stop(self):
    try:
      self._zk_client.delete(self._this_host_full_path())
    except NoNodeError:
      pass

  def _jittered_sleep(self):
    sleep(random.uniform(0, self._jitter_range))

  def _monitor_kill_instance(self):
    sleep(self._disconnected_timeout)
    with participation_lock:
      self.currently_watch_disconnect = False
      if not self.participating:
        # still not participating
        # meaning has been disconnected for too long
        raise DisconnectedTooLongError()
