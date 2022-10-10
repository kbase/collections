import inspect
import json
import yaml
import os

GLOBAL_CONFIG_FILE = 'loaders_config.yml'


class ConfigParser:

    def __init__(self, env='production'):
        self.env = env
        self.config = self.parse_config(self.env)

    def parse_config(self, env):
        caller_frame = inspect.stack()[2]
        caller_filename_full = caller_frame.filename
        caller_file_dir = os.path.dirname(caller_filename_full)

        callee_frame = inspect.stack()[1]
        callee_filename_full = callee_frame.filename
        callee_file_dir = os.path.dirname(callee_filename_full)

        config = dict()
        # parse global config
        global_config = self._parse_file(os.path.join(callee_file_dir, GLOBAL_CONFIG_FILE))[env]
        config.update(global_config)

        # parse collection config
        collection_config_file = [f for f in os.listdir(caller_file_dir) if f.lower().endswith('.yml') or f.lower().endswith('.yaml')]

        if len(collection_config_file) > 1:
            raise ValueError("Expecting ONE and only ONE collection config file. Received: {}".format(
                len(collection_config_file)))
        elif len(collection_config_file) == 0:
            # no collection specific config file
            pass
        else:
            # parse collection specific config
            collection_config_file = collection_config_file[0]
            coll_config = self._parse_file(os.path.join(caller_file_dir, collection_config_file))
            config.update(coll_config)

        return config

    @staticmethod
    def _parse_file(file_path):

        with open(file_path, 'r') as stream:
            try:
                data = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                raise ValueError('Cannot read config file') from exc

        return data
