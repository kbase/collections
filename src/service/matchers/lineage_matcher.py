"""
Matches assemblies and genomes to collections based on the GTDB lineage string.
"""

import asyncio

from pydantic import BaseModel, Field
from typing import Any, Callable

from src.common.storage.collection_and_field_names import FLD_GENOME_ATTRIBS_GTDB_LINEAGE
from src.service import errors
from src.service.app_state import PickleableStorage
from src.service.match_processing import MatchProcess
from src.service.data_products import genome_attributes
from src.service.matchers.common_models import Matcher
from src.service.storage_arango import ArangoStorage


# See the workspace specs for the types listed in the matcher
_GTDB_LINEAGE_METADATA_KEY = "GTDB_lineage"
_GTDB_LINEAGE_VERSION_METADATA_KEY = "GTDB_source_ver"

_COLLECTION_PARAMS_GTDB_VERSION_KEY = "gtdb_version"


class GTDBLineageMatcherCollectionParameters(BaseModel):
    "Parameters for the GTDB lineage matcher."
    gtdb_version: str = Field(
        example="207.0",
        description="The GTDB version of the collection in which the matcher is installed. " +
            "Input data to the matcher must match this version of GTDB or the match will " +
            "abort.",
        regex=r"^\d{2,4}\.\d{1,2}$"  # giving a little room for expansion
    )


async def _process_match_async(match_id: str, pstorage: PickleableStorage, args: list[list[str]]):
    lineages = args[0]
    print(f"Got {len(lineages)} lineages for match {match_id}")
    arangoclient, storage = await pstorage.get_storage()
    try:
        # Could save some bandwidth here buy adding a method to just get the internal ID
        # Microoptimization, wait until it's a problem
        match = await storage.get_match(match_id)
        print(match)
        match = await storage.get_match(match_id, verbose=True)
        print(match)
        match = await storage.get_match_full(match_id)
        print(match)
        # TODO MATCHERS make the callable actually do stuff
        # TODO MATCHERS remove partial lineages if they don't extend to the specified rank
    finally:
        await arangoclient.close()


def _process_match(match_id: str, pstorage: PickleableStorage, args: list[list[str]]):
    asyncio.run(_process_match_async(match_id, pstorage, args))


class GTDBLineageMatcher(Matcher):

    def generate_match_process(self,
        metadata: dict[str, dict[str, Any]],
        collection_parameters: dict[str, Any],
        # TODO MATCHERS user parameters when needed
    ) -> MatchProcess:
        """
        The method checks that input metadata allows for calculating the match and throws
        an exception if that is not the case; otherwise returns a MatchProcess that allows
        calculating the match.

        metadata - the workspace metadata of the objects to match against, mapped by its UPA.
        collection_parameters - the parameters for this match from the collection specification.
            It it expected that the parameters have been validated against the matcher schema
            for said parameters.
        """
        lineages = []
        for upa, meta in metadata.items():
            # Assume that if the lineage exists in the metadata it's in the correct format.
            # It's added as autometadata from the object created by an uploader so this should
            # be true.
            # If it turns out that someone is adding bad lineage information to the workspace
            # objects add a more rigorous parser
            if not meta.get(_GTDB_LINEAGE_METADATA_KEY):
                raise errors.MissingLineageError(
                    f"Object {upa} is missing lineage metadata in key {_GTDB_LINEAGE_METADATA_KEY}"
                )
            lin_ver = meta.get(_GTDB_LINEAGE_VERSION_METADATA_KEY)
            if not lin_ver:
                raise errors.MissingLineageError(
                    f"Object {upa} is missing lineage version metadata in key "
                    + f"{_GTDB_LINEAGE_VERSION_METADATA_KEY}"
                )
            coll_lin_ver = collection_parameters[_COLLECTION_PARAMS_GTDB_VERSION_KEY]
            # May want to do some heuristics here to more fuzzily match, e.g. r207.0 and 207.0
            # should match.
            if lin_ver != coll_lin_ver:
                raise errors.LineageVersionError(
                    f"Object {upa} lineage version is {lin_ver}, while the collection's version "
                    + f"is {coll_lin_ver}"
                )
            lineages.append(meta[_GTDB_LINEAGE_METADATA_KEY])

        return MatchProcess(process=_process_match, args=[lineages])


MATCHER = GTDBLineageMatcher(
    id="gtdb_lineage",
    description="Matches based on the GTDB lineage string. Requires the GTDB lineage to be "
        + f"in the '{FLD_GENOME_ATTRIBS_GTDB_LINEAGE}' field in the genome attributes data "
        + "product.",
    types=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
    required_data_products=[genome_attributes.ID],
    user_parameters=None, # TODO MATCHERS add rank parameter when supporting rank based matching
    collection_parameters=GTDBLineageMatcherCollectionParameters.schema()
)
