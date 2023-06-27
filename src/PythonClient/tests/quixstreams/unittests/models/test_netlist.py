import unittest

import pytest

from src.quixstreams.models.netlist import NetList


@pytest.mark.skip('TODO: Fix the tests')
class NetListTests(unittest.TestCase):

    def test_constructor_for_string(self):
        net_list = NetList.constructor_for_string()

        # no exception should be raised

    def test_setitem(self):
        net_list = NetList.constructor_for_string()
        net_list.append("test")

        net_list[0] = "test 3"

        self.assertEqual(net_list[0], "test 3")

    def test_delitem_single(self):
        net_list = NetList.constructor_for_string()

        net_list.append("test")
        net_list.append("test 2")
        net_list.append("test 3")

        self.assertEqual(net_list.count(), 3)

        del net_list[1]

        self.assertEqual(net_list.count(), 2)

        self.assertEqual(net_list[0], "test")
        self.assertEqual(net_list[1], "test 3")

    # def test_delitem_slice(self): # TODO
    #     net_list = NetList.constructor_for_string()
    #
    #     net_list.append("test")
    #     net_list.append("test 2")
    #     net_list.append("test 3")
    #     net_list.append("test 4")
    #
    #     self.assertEqual(net_list.count(), 4)
    #
    #     del net_list[1:2]
    #
    #     self.assertEqual(net_list.count(), 2)
    #
    #     self.assertEqual(net_list[0], "test")
    #     self.assertEqual(net_list[1], "test 4")

    def test_append(self):
        net_list = NetList.constructor_for_string()

        net_list.append("test")

        self.assertEqual(net_list[0], "test")

    def test_remove(self):
        net_list = NetList.constructor_for_string()

        net_list.append("test")
        net_list.append("test 2")
        net_list.append("test 3")

        self.assertEqual(net_list.count(), 3)

        net_list.remove("test 2")

        self.assertEqual(net_list.count(), 2)
        self.assertEqual(net_list[0], "test")
        self.assertEqual(net_list[1], "test 3")

    def test_clear(self):
        net_list = NetList.constructor_for_string()
        net_list.append("test")
        net_list.append("test 2")
        net_list.append("test 3")

        self.assertEqual(net_list.count(), 3)

        net_list.clear()

        self.assertEqual(net_list.count(), 0)


@pytest.mark.skip('TODO: Fix the tests')
class NetReadOnlyListTests(unittest.TestCase):

    def test_getitem(self):
        net_list = NetList.constructor_for_string()
        net_list.append("test")

        self.assertEqual(net_list[0], "test")

    def test_contains(self):
        net_list = NetList.constructor_for_string()
        net_list.append("test")

        self.assertEqual("test" in net_list, True)
        self.assertEqual("test 2" in net_list, False)

    def test_count(self):
        net_list = NetList.constructor_for_string()
        net_list.append("test")

        self.assertEqual(net_list.count(), 1)

        net_list.append("test 2")
        net_list.append("test 3")
        net_list.append("test 4")

        self.assertEqual(net_list.count(), 4)

    def test_iter(self):
        net_list = NetList.constructor_for_string()
        net_list.append("test")
        net_list.append("test 2")
        net_list.append("test 3")

        python_list = ["", "", ""]
        for i, v in enumerate(net_list):
            python_list[i] = v

        self.assertEqual(python_list[0], "test")
        self.assertEqual(python_list[1], "test 2")
        self.assertEqual(python_list[2], "test 3")

    def test_iter2(self):
        net_list = NetList.constructor_for_string()
        net_list.append("test")
        net_list.append("test 2")
        net_list.append("test 3")

        pythonlist = []
        for v in net_list:
            pythonlist.append(v)

        self.assertEqual(pythonlist[0], "test")
        self.assertEqual(pythonlist[1], "test 2")
        self.assertEqual(pythonlist[2], "test 3")
