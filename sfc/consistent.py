from binascii import crc32
from threading import RLock

from sfc.util.exceptions import LocateEmpty

class Consistent(object):
  """
  Implementation of consistent hashing, used to have a deterministic position of all hosts

  Internally, this class is using binascii.crc32 to map host's position

  This implementation is thread-safe
  """

  def __init__(self, hosts = []):
    """
    hosts will be the locations of each instance of sfc, including this one
    """
    self.lock = RLock()
    self._host_pos = []
    self._host_map = {}
    self.add_many(hosts)

  def clear(self):
    with self.lock:
      self._host_pos.clear()
      self._host_map.clear()

  def add(self, host):
    with self.lock:
      hashval = self.host_as_crc32(host)
      if hashval not in self._host_map:
        self._host_pos.append(hashval)
        self._host_map[hashval] = host
        self._host_pos = sorted(self._host_pos)

  def add_many(self, hosts):
    with self.lock:
      for host in hosts:
        hashval = self.host_as_crc32(host)
        if hashval not in self._host_map:
          self._host_pos.append(hashval)
          self._host_map[hashval] = host

      self._host_pos = sorted(self._host_pos)

  def locate(self, target):
    with self.lock:
      if len(self._host_pos) == 0:
        raise LocateEmpty("no host available, can't match to anything")
      
      hashval = self.host_as_crc32(target)
      for host in self._host_pos:
        if hashval < host:
          return self._host_map[host]

      # not found until list is finished,
      # choose the first one, as the consistenthash basically rolls over
      return self._host_map[self._host_pos[0]]
     
  def remove(self, host):
    with self.lock:
      hashval = self.host_as_crc32(host)
      if hashval in self._host_map:
        self._host_map.pop(hashval)
        self._host_pos.remove(hashval)

  def host_as_crc32(self, host):
    try:
      hashval = crc32(host.encode())
    except AttributeError as ae:
      # just for better error message
      raise AttributeError("host should implement `encode()` to bytes")

    return hashval

  def reset_with_new(self, hosts):
    with self.lock:
      self.clear()
      self.add_many(hosts)
