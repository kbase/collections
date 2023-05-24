import asyncio
import subprocess
from pathlib import Path

PMI_FILES = "PMI_files_list.txt"
SKETCH_DIR = "./PMI_individual_sketches"


# https://stackoverflow.com/a/61478547/643675
async def gather_with_concurrency(n, coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro
    return await asyncio.gather(*(sem_coro(c) for c in coros))


async def sketch1(file):
    subprocess.run([
        "/global/homes/g/gaprice/mash/mash-Linux64-v2.3/mash",
        "sketch",
        "-o", "./PMI_individual_sketches/" + Path(file).name,
        "-k", "19",
        "-s", "10000",
        file
    ])

async def main():
    with open(PMI_FILES) as filelist:
        files = [f.strip() for f in filelist]
    coros = [sketch1(f) for f in files]
    await gather_with_concurrency(4, coros)


if __name__ == "__main__":
    asyncio.run(main())
