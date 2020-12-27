# sfdc
Distributed implementation of singleflight, written in python

By default, it uses consistent hashing on top of [zookeeper](https://zookeeper.apache.org/) + [kazoo](https://github.com/python-zk/kazoo) to handle service discovery
and use [falcon](https://falconframework.org/) web framework for backend server

This implementation is eventually consistent, in which during any request lifetime,
the instances known to each one may be different, but the `force_this_node` paramater is used
on backend call path, so it would never live-locking, calling each other
believing the other is still the rightful owner.
This approach works because zookeeper will return a snapshot of instances, in which the requested node
might be the rightful owner of the key, one time in the past.

See [tests/core.py](https://github.com/aarondwi/sfdc/blob/main/tests/core.py) for example on how to initialize and use this implementation.

## TODO
---------------------------------------------------

1. Handle case disconnected from topology_service for too long (kill?)
2. Add proper logging
3. Add shutdown mechanism
