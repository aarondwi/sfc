class StateInvalidException(BaseException):
  pass

class SfdcFetchError(BaseException):
  """
  Thrown when sfdc failed to fetch from proper host in consistent
  """
  pass

class LocateEmpty(BaseException):
  """
  Thrown when consistent has no host, so cant locate anything
  """
  pass
