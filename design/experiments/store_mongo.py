# A Q&D script for storing sourmash sketches in MongoDB documents, then
# fetching them back and searching them.

# This may not work for metagenome signatures depending on their size.

# Expects an unauthed mongod instance running on the default port

import os
import pymongo
import sys

from pathlib import Path


EXT_SIG = ".sig"
DB_NAME = "sourmash"
COLL_NAME = "base64"
KEY_FILE_SIGNATURE = "sig"
KEY_FILE_NAME = "file"
KEY_FILE_LEN = "len"


def store(coll: pymongo.collection.Collection, sig_path: Path):
    for p in os.listdir(sig_path):
        p = Path(p)
        if p.suffix == EXT_SIG:
            with open(sig_path / p, "rb") as f:
                b = f.read()
                coll.insert_one({
                    KEY_FILE_SIGNATURE: b,
                    KEY_FILE_NAME: str(p),
                    KEY_FILE_LEN: len(b),
                })


def main():
    with pymongo.MongoClient() as cli:
        if sys.argv[1] == "store":
            store(
                cli.get_database(DB_NAME).get_collection(COLL_NAME),
                Path(sys.argv[2])
            )


if __name__ == "__main__":
    main()
