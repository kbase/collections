# A Q&D script for storing sourmash sketches in MongoDB documents, then
# fetching them back and searching them.

# This may not work for metagenome signatures depending on their size.

# Expects an unauthed mongod instance running on the default port

import os
import pymongo
import subprocess
import sys

from pathlib import Path


EXT_SIG = ".sig"
DB_NAME = "sourmash"
COLL_NAME = "base64"  # this collection name no longer makes sense but whatever, it's test code
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


def sketch(coll: pymongo.collection.Collection, query_signature: Path, work_dir: Path):
    for d in coll.find({}, {KEY_FILE_LEN: 0}):
        with open(work_dir / d[KEY_FILE_NAME], "wb") as f:
            f.write(d[KEY_FILE_SIGNATURE])
    subprocess.run(["sourmash", "search", "-n", "0", "-t", "0.5", str(query_signature), str(work_dir)])


def main():
    with pymongo.MongoClient() as cli:
        db = cli.get_database(DB_NAME)
        if sys.argv[1] == "store":
            # args = signature location
            store(db.get_collection(COLL_NAME), Path(sys.argv[2]))
        elif sys.argv[1] == "get":
            # args = query signature location, dir to download target signatures
            sketch(db.get_collection(COLL_NAME), Path(sys.argv[2]), Path(sys.argv[3]))


if __name__ == "__main__":
    main()
