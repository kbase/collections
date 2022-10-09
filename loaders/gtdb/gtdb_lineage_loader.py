from loaders.config_parser import ConfigParser


class GTDBLineageLoader:

    def __init__(self, env='production'):
        self.env = env
        config_parser = ConfigParser(self.env)
        self.config = config_parser.config
