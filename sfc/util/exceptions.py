class StateInvalidException(BaseException):
  pass

class FetchError(BaseException):
  """
  Raised when sfc failed to fetch from proper host in consistent
  """
  pass

class LocateEmpty(BaseException):
  """
  Raised when consistent has no host, so cant locate anything
  """
  pass

class ListNotValidError(BaseException):
  """
  Raised when disconnected from Topology Service longer than timeout,
  so the content is considered stale
  """
  pass
