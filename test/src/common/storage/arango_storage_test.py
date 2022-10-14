import unittest
from src.common.storage.arango_storage import ArangoStorage


class ArangoStorageTest(unittest.TestCase):

    def setUp(self) -> None:
        print('starting arango storage test')
        # TODO this is a no-op test

    def test_no_op(self):
        assert "hello arango"
