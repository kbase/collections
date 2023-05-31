"""
Q&D script to test the effect of parallel requests on the KBase sketch service.

Note that the sketch service does no internal caching and so we use the same assembly ID
over and over for parallel requests.
"""

import aiohttp
import asyncio
from collections import defaultdict
import functools
import os
import sys
import time
import uuid

from src.service.sdk_async_client import SDKAsyncClient, ServerError


# Note dynamic, look up from the catalog web services UI
SKETCH_URL = "https://ci.kbase.us:443/dynserv/d951457d6b973755217a65866f5d39b8544e4632.sketch-service"
OBJECT_ID = "68981/811/1"  # Genome
#OBJECT_ID = "68981/397/1"  # Assembly

# The homology service is currently limited to 32 cores
SIZES = [i * 10 + 10 for i in range(10)]


async def _request(client, token, id_):
    t = time.time()
    print(f"Starting request id {id_} at {t:.3f} sec")
    try:
        res = await client.call(
            "get_homologs",
            params={
                "ws_ref": OBJECT_ID,
                "search_db": "NCBI_Refseq2",  # avoid ID Mapping lookup
                "n_max_results": 1000,  # same as homology service
                "bypass_caching": True,
            },
            token=token
        )
        t = time.time()
        print(f"Completed request id {id_} at {t:.3f} sec")
        assert res["impl"] == "mash"  # check that we're getting actual results
        return None
    except ServerError as e:
        return e.message
    except aiohttp.client_exceptions.ClientResponseError as e:
        return f"Code: {e.status} message: {e.message}"
    except asyncio.TimeoutError as e:
        return "Asyncio timeout"


async def _run_parallel(client, token, size):
    print(f"Performing {size} parallel requests")
    results = []
    start = time.time()
    async with asyncio.TaskGroup() as tg:
        for i in range(size):
            results.append(tg.create_task(_request(client, token, i + 1)))
    elapsed = time.time() - start
    errortypes = defaultdict(int)
    for res in results:
        r = res.result()
        if r is not None:
            print(r)
            if "assemblyhomologyservice" in r.lower():
                errortypes["Unspecified AssemblyHomology failure"] += 1
            else:
                errortypes[r] += 1
    print(f"{sum(errortypes.values())} errors out of {size} tasks in {elapsed} sec")
    return elapsed, errortypes


async def main():
    token_env_var = sys.argv[1]
    token = os.environ[token_env_var]
    print(f"performing parallel requests to {SKETCH_URL}")
    client = SDKAsyncClient(SKETCH_URL)
    table = []
    try:
        for s in SIZES:
            elapsed, errortypes = await _run_parallel(client, token, s)
            table.append((s, elapsed, errortypes))
    finally:
        await client.close()
    print("\nReqs\tTime\tErrors\t%")
    for size, elapsed, errortypes in table:
        errs = sum(errortypes.values())
        print(f"{size}\t{elapsed:.02f}\t{errs}\t{100 * errs / size:.02f}")
    print("\nErrortypes")
    for size, elapsed, errortypes in table:
        print(f"Requests: {size}")
        if not errortypes:
            print("\tNo errors")
        else:
            for e in sorted(errortypes):
                print(f"\t{e}\t{errortypes[e]}")


if __name__ == "__main__":
    asyncio.run(main())
