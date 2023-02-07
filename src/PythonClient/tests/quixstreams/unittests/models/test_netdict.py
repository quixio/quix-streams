import unittest
from src.quixstreams.models.netdict import NetDict, ReadOnlyNetDict

class NetDictTests(unittest.TestCase):

    def test_constructor_for_string(self):
        net_dict = NetDict.constructor_for_string_string()

        # no exception should be raised

    def test_setitem(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict["test"] = "abracadabra"

        self.assertEqual(net_dict["test"], "abracadabra")

    def test_deltitem(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict["test"] = "abracadabra"

        del net_dict["test"]

        self.assertEqual(net_dict["test"], None)

    def test_update(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict.update("test", "something")
        net_dict.update("test", "abracadabra")

        self.assertEqual(net_dict["test"], "abracadabra")

    def test_clear(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict.update("test 1", "one")
        net_dict.update("test 2", "two")

        self.assertEqual(len(net_dict), 2)

        net_dict.clear()

        self.assertEqual(len(net_dict), 0)

    def test_pop(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict.update("test 1", "one")

        self.assertEqual(len(net_dict), 1)

        result = net_dict.pop("test 1")

        self.assertEqual(len(net_dict), 0)
        self.assertEqual(result, "one")

    def test_setdefault(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict.update("test 1", "one")

        self.assertEqual(len(net_dict), 1)

        result = net_dict.setdefault("test 1", "other")

        self.assertEqual(len(net_dict), 1)
        self.assertEqual(result, "one")

        result = net_dict.setdefault("test 2", "two")
        self.assertEqual(len(net_dict), 2)
        self.assertEqual(result, "two")


class ReadOnlyNetDictTests(unittest.TestCase):

    def test_len(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict.update("test 1", "one")

        self.assertEqual(len(net_dict), 1)

        result = net_dict.setdefault("test 1", "other")

        self.assertEqual(len(net_dict), 1)
        self.assertEqual(result, "one")

        result = net_dict.setdefault("test 2", "two")
        self.assertEqual(len(net_dict), 2)
        self.assertEqual(result, "two")

    def test_keys(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict.update("test 1", "one")

        keys = net_dict.keys()

        self.assertEqual(1, len(keys))
        self.assertEqual("test 1", keys[0])

    def test_values(self):
        net_dict = NetDict.constructor_for_string_string()

        net_dict.update("test 1", "one")

        values = net_dict.values()

        self.assertEqual(1, len(values))
        self.assertEqual("one", values[0])