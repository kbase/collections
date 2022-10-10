import unittest
from loaders.gtdb.gtdb_lineage_loader import GTDBLineageLoader


class GTDBLineageLoaderTest(unittest.TestCase):

    def setUp(self) -> None:
        print('starting gtdb lineage loader test')
        self.env = 'test'
        self.gtdb_loader = GTDBLineageLoader(self.env)

    def test_gtdb_lineage_loader_config(self):
        config_gtdb = self.gtdb_loader.config
        expected_global_configs = ['HOST', 'DATABASE', 'USERNAME', 'PASSWORD', 'ARANGO_COLLECTION']
        expected_gtdb_configs = ['LOAD_VERSION', 'KBASE_COLLECTION']

        assert set(expected_global_configs) < set(config_gtdb.keys())
        assert set(expected_gtdb_configs) < set(config_gtdb.keys())

        expected_host = 'http://localhost:48000'
        assert config_gtdb['HOST'] == expected_host

        expected_load_version = 207
        assert config_gtdb['LOAD_VERSION'] == expected_load_version
