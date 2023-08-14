# A Q&D script for storing sourmash sketches in Arango documents, then
# fetching them back and searching them.

# This may not work for metagenome signatures depending on their size. See
# https://github.com/arangodb/arangodb/issues/10754

# Expects an unauthed arangod instance running on the default port with the given database and
# collection already created.

import aioarango
import asyncio
import base64
import os
import subprocess
import sys

from pathlib import Path


EXT_SIG = ".sig"
DB_NAME = "sourmash"
COLL_NAME_INT_ARRAY = "test"
COLL_NAME_BASE64 = "base64"
KEY_FILE_SIGNATURE = "sig"
KEY_FILE_NAME = "file"
KEY_FILE_LEN = "len"

ZERO_BYTE = (0).to_bytes()


async def store(coll: aioarango.collection.Collection, sig_path: Path):
    for p in os.listdir(sig_path):
        p = sig_path / p
        if p.suffix == EXT_SIG:
            with open(p, "rb") as f:
                b = f.read()
                lenb = len(b)
                # see https://github.com/arangodb/arangodb/issues/107#issuecomment-1068482185
                int64s = []
                remainder = lenb % 8
                last = None 
                if remainder:
                    last = b[-remainder:]
                    b = b[:-remainder]
                    last += b"".join([ZERO_BYTE] * (8 - remainder))
                for i in range(0, len(b), 8):
                    int64s.append(int.from_bytes(b[i: i + 8]))
                if last:
                    int64s.append(int.from_bytes(last))
                await coll.insert({
                    KEY_FILE_SIGNATURE: int64s,
                    KEY_FILE_NAME: str(p.name),
                    KEY_FILE_LEN: lenb,
                })


async def storeb64(coll: aioarango.collection.Collection, sig_path: Path):
    for p in os.listdir(sig_path):
        p = sig_path / p
        if p.suffix == EXT_SIG:
            with open(p, "rb") as f:
                b = f.read()
                b64string = base64.b64encode(b).decode("utf-8")
                await coll.insert({
                    KEY_FILE_SIGNATURE: b64string,
                    KEY_FILE_NAME: str(p.name),
                    KEY_FILE_LEN: len(b),
                })


async def sketch(coll: aioarango.collection.Collection, query_signature: Path, work_dir: Path):
    async for d in await coll.find({}):
        with open(work_dir / d[KEY_FILE_NAME], "wb") as f:
            last = d[KEY_FILE_SIGNATURE].pop().to_bytes(length=8)
            for int64 in d[KEY_FILE_SIGNATURE]:
                f.write(int64.to_bytes(length=8))
            if remainder := d[KEY_FILE_LEN] % 8:
                last = last[:remainder]
            f.write(last)
    _sketch(query_signature, work_dir)


def _sketch(query_signature: Path, work_dir: Path):
    subprocess.run(
        ["sourmash", "search", "-n", "0", "-t", "0.5", str(query_signature), str(work_dir)])


async def sketch64(coll: aioarango.collection.Collection, query_signature: Path, work_dir: Path):
    async for d in await coll.find({}):
        with open(work_dir / d[KEY_FILE_NAME], "wb") as f:
            f.write(base64.b64decode(d[KEY_FILE_SIGNATURE].encode("utf-8")))
    _sketch(query_signature, work_dir)


async def main():
    cli = None
    try:
        cli = aioarango.ArangoClient()
        db = await cli.db(DB_NAME, verify=True)
        if sys.argv[1] == "store":
            # args = signature location
            await store(db.collection(COLL_NAME_INT_ARRAY), Path(sys.argv[2]))
        if sys.argv[1] == "storeb64":
            # args = signature location
            await storeb64(db.collection(COLL_NAME_BASE64), Path(sys.argv[2]))
        elif sys.argv[1] == "get":
            # args = query signature location, dir to download target signatures
            await sketch(db.collection(COLL_NAME_INT_ARRAY), Path(sys.argv[2]), Path(sys.argv[3]))
        elif sys.argv[1] == "getb64":
            # args = query signature location, dir to download target signatures
            await sketch64(db.collection(COLL_NAME_BASE64), Path(sys.argv[2]), Path(sys.argv[3]))
        else:
            raise ValueError("unknown command: " + sys.argv[1])
    finally:
        if cli:
            await cli.close()


if __name__ == "__main__":
    asyncio.run(main())
