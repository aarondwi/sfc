from binascii import crc32

class Consistent(object):
  """
  Implementation of consistent hashing, used to have a deterministic position of all hosts

  Internally, this class is using binascii.crc32 to map host's position

  This class' implementation is NOT thread-safe
  """

  def __init__(self, hosts = []):
    """
    hosts will be the locations of each instance of sfdc, including this one
    """
    self._host_pos = []
    self._host_map = {}
    self.add_many(hosts)

  def clear(self):
    self._host_pos.clear()
    self._host_map.clear()

  def add(self, host):
    """
    add the `host` into internal DS, do nothing if already exist
    """
    hashval = self.host_as_crc32(host)
    if hashval not in self._host_map:
      self._host_pos.append(hashval)
      self._host_map[hashval] = host
      self._host_pos = sorted(self._host_pos)

  def add_many(self, hosts):
    for host in hosts:
      self.add(host)

  def locate(self, target):
    if len(self._host_pos) == 0:
      raise LookupError("no host available, can't match to anything")
    
    hashval = self.host_as_crc32(target)
    for host in self._host_pos:
      if hashval < host:
        return self._host_map[host]

    # not found until list is finished,
    # choose the first one, as the consistenthash basically rolls over
    return self._host_map[self._host_pos[0]]
     

  def remove(self, host):
    hashval = self.host_as_crc32(host)
    if hashval in self._host_map:
      self._host_map.pop(hashval)
      self._host_pos.remove(hashval)

  def host_as_crc32(self, host):
    try:
      hashval = crc32(host.encode())
    except AttributeError as ae:
      raise ValueError("host should implement `encode()` to bytes")

    return hashval
