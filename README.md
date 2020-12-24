# sfdc
Distributed implementation example of singleflight, written in python

## TODO
---------------------------------------------------

1. Handle case disconnected from topology_service for too long (kill?)
2. Handle if the target url returns failure
3. Add proper logging
4. allow accepting WSGI server, which only be passed falcon.API() object
