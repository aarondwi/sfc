class StateInvalidException(BaseException):
  pass

class SfcFetchError(BaseException):
  """
  Thrown when sfc failed to fetch from proper host in consistent
  """
  pass

class LocateEmpty(BaseException):
  """
  Thrown when consistent has no host, so cant locate anything
  """
  pass
