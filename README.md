# sfdc
Distributed implementation example of singleflight, written in python

By default, it uses Zookeeper + Kazoo Library to handle service discovery,
use consistent hashing with crc32 to get integer value,
and use `falcon` web framework for backend server

This implementation is eventually consistent, in which during any request lifetime,
the nodes on all instance may be different, but the `force_this_node` paramater is used
on backend call path, so it would never live-locking, calling each other
believing the other is still the rightful owner.
This approach works because zookeeper will return a snapshot of instances, in which the node
at a time in the past, may be the rightful owner of the key

See `tests/core.py` for example on how to use it.

## TODO
---------------------------------------------------

1. Handle case disconnected from topology_service for too long (kill?)
2. Add proper logging
3. allow accepting WSGI server, which internally will only be passed falcon.API() object
4. Add shutdown mechanism
