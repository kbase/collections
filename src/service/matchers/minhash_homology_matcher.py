"""
Matches assemblies and genomes to collections based on Minhash homology.
"""

# NOTE: this is only demo worthy as of now. See the benchmarks in /design/experiments,
#       but the sketch / homology services
#           a) can only handle ~ 40 simultaneous connections before errors start occurring, so
#               multiple simultaneous matches are likely to fail and
#           b) time out after ~ 100 seconds. Sketching 1 sequence against 100k takes about 20-30s
#              so larger collections will be likely to time out.
#           c) depend on `mash`, which appears to be abandonware
#       Longer term, the implementation probably needs to be moved to an app that downloads
#       the sketch database for the collection from somewhere and downloads the genomes, sketches
#       them, and runs the sketches against the collection. It also should use sourmash
#       which seems to be the community accepted application.

import asyncio
import logging

from pydantic import ConfigDict, BaseModel, Field, HttpUrl

from src.service import data_product_specs
from src.service import models
from src.service.app_state_data_structures import PickleableDependencies
from src.service.data_products import genome_attributes
from src.service.matchers.common_models import Matcher
from src.service.processing import CollectionProcess, Heartbeat, HEARTBEAT_INTERVAL_SEC
from src.service.sdk_async_client import SDKAsyncClient, ServerError
from src.service.storage_arango import ArangoStorage

from typing import Any


_SKETCH_SERVICE = "sketch_service"
_SERVICE_WIZARD = "ServiceWizard.get_service_status"
_MAX_SKETCH_CONNECTIONS = 10  # > 40 or so makes the mash binary unhappy
_DEFAULT_MAX_DIST = 0.5


class MinHashHomologyMatcherCollectionParameters(BaseModel):
    "Parameters for the Minhash homology matcher."
    service_wizard_url: HttpUrl = Field(
        example="https://ci.kbase.us/services/service_wizard",
        description="The URL of the service wizard to use to look up the sketch service URL."
    )
    sketch_database_name: str = Field(
        example="PMI_2023.1",
        description="The name of the sketch database in the Assembly Homology Service to match "
            + "against. This parameter is sent to the sketch service."
    )
    model_config = ConfigDict(extra="forbid")


class MinHashHomologyMatcherUserParameters(BaseModel):
    "User parameters for the Minhash homology matcher."
    maximum_distance: float | None = Field(
        default=_DEFAULT_MAX_DIST,
        example=0.2,
        ge=0,
        le=0.5,  # Assembly homology service max
        description="The maximum minhash distance to consider for matches."
    )
    # TODO HOMOLOGY_MATCHER may want to support more parameters if we switch to a different
    #                       implementation like sourmash
    model_config = ConfigDict(extra="forbid")


async def _get_sketch_service_client(collection_parameters: dict[str, Any]):
    swz_url = collection_parameters["service_wizard_url"]
    swzcli = SDKAsyncClient(swz_url)
    try:
        swzstatus = await swzcli.call(
            # don't worry about the service version for now
            _SERVICE_WIZARD, params=[{"module_name": _SKETCH_SERVICE, "version": None}])
    finally:
        await swzcli.close()
    return SDKAsyncClient(swzstatus["url"])


async def _perform_sketch(
    sketch_cli: SDKAsyncClient,
    search_db: str,
    upa: str,
    token: str,
    max_dist: float,
):
    try:
        res = await sketch_cli.call(
            "get_homologs",
            params={
                "ws_ref": upa,
                "search_db": search_db, 
                "n_max_results": 1000,
                # TODO HOMOLOGY_MATCHER send distance if sketch service or next impl accepts
            },
            token=token
        )
    except ServerError as e:
        raise ValueError(f"Error from sketch service for UPA {upa}: {e.message}") from e
    return {d["sourceid"] for d in res["distances"] if d["dist"] <= max_dist}


async def _get_kbase_ids(
    upas: list[str],
    sketch_cli: SDKAsyncClient,
    search_db: str,
    token: str,
    max_dist: float,
):
    results = []
    semaphore = asyncio.Semaphore(_MAX_SKETCH_CONNECTIONS)
    async def sem_coro(coro):
        async with semaphore:
            return await coro
    # Some kind of % complete for the user seems doable and might be nice. Maybe later.
    async with asyncio.TaskGroup() as tg:
        for upa in upas:
            results.append(tg.create_task(sem_coro(
                _perform_sketch(sketch_cli, search_db, upa, token, max_dist)
            )))
    kbids = set()
    for r in results:
        kbids |= r.result()
    return sorted(kbids)


async def _do_match(  # this method sig is getting real long
    deps: PickleableDependencies,
    internal_match_id: str,
    storage: ArangoStorage,
    upas: list[str],
    collection_params: dict[str, Any],
    token: str,
    max_dist: float,
):
    match = await storage.get_match_by_internal_id(internal_match_id)
    # use version number to avoid race conditions with activating collections
    coll = await storage.get_collection_version_by_num(match.collection_id, match.collection_ver)
    search_db = collection_params["sketch_database_name"]
    sketch_cli = await _get_sketch_service_client(collection_params)
    try:
        kbase_ids = await _get_kbase_ids(upas, sketch_cli, search_db, token, max_dist)
    finally:
        await sketch_cli.close()
    # hardcoding genome attributes again... this might BMITA later
    genome_attribs = data_product_specs.get_data_product_spec(genome_attributes.ID)
    await genome_attribs.apply_match(deps, storage, coll, internal_match_id, kbase_ids)


async def _process_match(
    internal_match_id: str,
    deps: PickleableDependencies,
    args: tuple[list[str], dict[str, Any], str, float],
):
    # TODO CODE a lot of duplication here with other process. Combine somehow?
    #           look for anywhere else a heartbeat is used
    upas = args[0]
    collection_params = args[1]
    token = args[2]
    max_dist = args[3]
    hb = None
    arangoclient = None
    try:
        arangoclient, storage = await deps.get_storage()
        async def heartbeat(millis: int):
            await storage.send_match_heartbeat(internal_match_id, millis)
        hb = Heartbeat(heartbeat, HEARTBEAT_INTERVAL_SEC)
        hb.start()
        await _do_match(deps, internal_match_id, storage, upas, collection_params, token, max_dist)
    except Exception as _:
        logging.getLogger(__name__).exception(
            f"Matching process for match with internal ID {internal_match_id} failed")
        await storage.update_match_state(
            internal_match_id, models.ProcessState.FAILED, deps.get_epoch_ms())
    finally:
        if hb:
            hb.stop()
        if arangoclient:
            await arangoclient.close()


class MinhashHomologyMatcher(Matcher):

    def generate_match_process(
        self,
        internal_match_id: str,
        metadata: dict[str, dict[str, Any]],
        user_parameters: dict[str, Any],
        collection_parameters: dict[str, Any],
        token: str,
    ) -> CollectionProcess:
        """
        Returns a CollectionProcess that allows calculating the match.

        internal_match_id - the internal ID of the match.
        metadata - the workspace metadata of the objects to match against, mapped by its UPA.
        user_parameters - the parameters for this match provided by the user. It is expected that
           the parameters have been validated against the matcher schema for said parameters.
        collection_parameters - the parameters for this match from the collection specification.
            It it expected that the parameters have been validated against the matcher schema
            for said parameters.
        token - the user's token.
        """
        # No checks necessary since the calling code checks that the UPAs are accessible
        # and are assemblies and / or genomes, which is all we need
        max_dist = (user_parameters.get("maximum_distance")
                    if user_parameters else _DEFAULT_MAX_DIST)
        return CollectionProcess(
            process=_process_match,
            data_id=internal_match_id,
            args=[list(metadata.keys()), collection_parameters, token, max_dist]
        )
        # maybe we should use a service token here? not sure


MATCHER = MinhashHomologyMatcher(
    id="minhash_homology",
    description="Matches based on Minhash homology.",
    types=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
    set_types=[
        "KBaseSearch.GenomeSet",  # eventually supposed to be replaced by the KBaseSets version
        "KBaseSets.GenomeSet",
        "KBaseSets.AssemblySet",
    ],
    required_data_products=[genome_attributes.ID],
    user_parameters=MinHashHomologyMatcherUserParameters.model_json_schema(),
    collection_parameters=MinHashHomologyMatcherCollectionParameters.model_json_schema()
)
