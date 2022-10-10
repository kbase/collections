import unittest
from loaders.config_parser import ConfigParser


class ConfigParserTest(unittest.TestCase):

    def setUp(self) -> None:
        print('starting config builder test')
        self.env = 'test'
        self.configParser = ConfigParser(self.env)

    def test_config_env(self):
        assert self.env == self.configParser.env

    def test_global_config(self):

        config_global = self.configParser.config

        expected_keys = ['HOST', 'DATABASE', 'USERNAME', 'PASSWORD', 'ARANGO_COLLECTION']
        assert set(expected_keys) <= set(config_global.keys())

        expected_host = 'http://localhost:48000'
        assert config_global['HOST'] == expected_host

        expected_db = 'gavin_test'
        assert config_global['DATABASE'] == expected_db