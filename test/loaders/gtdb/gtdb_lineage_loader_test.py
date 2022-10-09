import unittest
from loaders.gtdb.gtdb_lineage_loader import GTDBLineageLoader


class GTDBLineageLoaderTest(unittest.TestCase):

    def setUp(self) -> None:
        print('starting gtdb lineage loader test')
        self.env = 'test'
        self.gtdb_loader = GTDBLineageLoader(self.env)

    def test_gtdb_lineage_loader_config(self):
        config_gtdb = self.gtdb_loader.config
        expected_global_configs = ['CONFIG_FILE', 'HOST', 'DATABASE', 'ARANGO_COLLECTION']
        expected_gtdb_configs = ['LOAD_VERSION', 'KBASE_COLLECTION', 'LOAD_FILES']

        assert set(expected_global_configs) < set(config_gtdb.keys())
        assert set(expected_gtdb_configs) < set(config_gtdb.keys())

        expected_config_file = '~/SCIENCE/taxonomy/gtdb/load_config.toml'
        assert config_gtdb['CONFIG_FILE'] == expected_config_file

        expected_load_version = 207
        assert config_gtdb['LOAD_VERSION'] == expected_load_version
