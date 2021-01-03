import unittest
from functools import partial

from sfc.consistent import Consistent
from sfc.util.exceptions import LocateEmpty

class TestConsistent(unittest.TestCase):
  def test_main_flow(self):
    hosts = [
      "http://127.0.0.1:7001",
      "http://127.0.0.1:7001",
      "http://127.0.0.1:7002",
      "http://127.0.0.1:7003",
    ]
    c = Consistent(hosts)
    self.assertEqual(len(c._host_pos), 3)

    self.assertEqual(
      c.locate("value1"),
      "http://127.0.0.1:7001"
    )
    self.assertEqual(
      c.locate("value2"),
      "http://127.0.0.1:7003"
    )
    self.assertEqual(
      c.locate("value3"),
      "http://127.0.0.1:7001"
    )

    c.remove("http://127.0.0.1:7001")
    self.assertEqual(len(c._host_pos), 2)
    self.assertEqual(
      c.locate("value1"),
      "http://127.0.0.1:7002"
    )

    c.clear()
    self.assertEqual(len(c._host_pos), 0)

  def test_locate_lookup_error(self):
    c = Consistent()
    self.assertRaises(
      LocateEmpty, 
      partial(c.locate, "notfound"))

  def test_host_cant_be_encoded(self):
    self.assertRaises(
      AttributeError, 
      partial(Consistent, [[]]))

  def test_reset_with_new(self):
    hosts = [
      "http://127.0.0.1:7001",
      "http://127.0.0.1:7001",
      "http://127.0.0.1:7002",
      "http://127.0.0.1:7003",
    ]

    c = Consistent(hosts)
    self.assertEqual(len(c._host_pos), 3)
    
    c.reset_with_new(["http://only_this_one"])
    self.assertEqual(len(c._host_pos), 1)
