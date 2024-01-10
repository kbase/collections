"""
Matches assemblies and genomes to collections based on the GTDB lineage string.
"""

import logging
import re
from typing import Any, Self

from pydantic import ConfigDict, BaseModel, Field, model_validator, conlist

from src.common.constants import GTDB_UNCLASSIFIED_PREFIX
from src.common.gtdb_lineage import GTDBRank, GTDBLineage, GTDBLineageError
from src.common.storage.collection_and_field_names import FLD_GENOME_ATTRIBS_GTDB_LINEAGE
from src.service import errors
from src.service import models
from src.service.app_state_data_structures import PickleableDependencies
from src.service.data_products import genome_attributes
from src.service.matchers.common_models import Matcher
from src.service.processing import CollectionProcess, Heartbeat, HEARTBEAT_INTERVAL_SEC

# See the workspace specs for the types listed in the matcher
_GTDB_LINEAGE_METADATA_KEY = "GTDB_lineage"
_GTDB_LINEAGE_VERSION_METADATA_KEY = "GTDB_source_ver"

_COLLECTION_PARAMS_GTDB_VERSIONS_KEY = "gtdb_versions"


class GTDBLineageMatcherCollectionParameters(BaseModel):
    "Parameters for the GTDB lineage matcher."
    gtdb_versions: conlist(str, min_length=1) = Field(
        example=["214.0", "214.1"],
        description="The allowed GTDB versions of the collection in which the matcher is installed. " +
            "Input data to the matcher must match one of the allowed versions of GTDB or the match will " +
            "abort.",
    )
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def _check_gtdb_versions(self) -> Self:
        pattern = r"^\d{2,4}\.\d{1,2}$"  # giving a little room for expansion
        for version in self.gtdb_versions:
            if not re.match(pattern, version):
                raise ValueError(f"Invalid version format: {version}")
        return self


class GTDBLineageMatcherUserParameters(BaseModel):
    "User parameters for the GTDB lineage matcher."
    gtdb_rank: GTDBRank = Field(
        default=GTDBRank.SPECIES,
        example=GTDBRank.SPECIES,
        description="A rank in the the GTDB lineage."
    )
    model_config = ConfigDict(extra="forbid")


async def _process_match(
    internal_match_id: str,
    deps: PickleableDependencies,
    args: tuple[list[str], bool]
):
    lineages = args[0]
    truncated = args[1]
    hb = None
    arangoclient = None
    try:
        arangoclient, storage = await deps.get_storage()
        async def heartbeat(millis: int):
            await storage.send_match_heartbeat(internal_match_id, millis)
        hb = Heartbeat(heartbeat, HEARTBEAT_INTERVAL_SEC)
        hb.start()
        # this might need to be configurable on the matcher to allow the matcher
        # to run against different data products
        await genome_attributes.perform_gtdb_lineage_match(
            internal_match_id, storage, lineages, truncated)
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


class GTDBLineageMatcher(Matcher):

    def generate_match_process(
        self,
        internal_match_id: str,
        metadata: dict[str, dict[str, Any]],
        user_parameters: dict[str, Any],
        collection_parameters: dict[str, Any],
        token: str,
    ) -> CollectionProcess:
        """
        The method checks that input metadata allows for calculating the match and throws
        an exception if that is not the case; otherwise returns a CollectionProcess that allows
        calculating the match.

        internal_match_id - the internal ID of the match.
        metadata - the workspace metadata of the objects to match against, mapped by its UPA.
        user_parameters - the parameters for this match provided by the user. It is expected that
           the parameters have been validated against the matcher schema for said parameters.
        collection_parameters - the parameters for this match from the collection specification.
            It it expected that the parameters have been validated against the matcher schema
            for said parameters.
        token - the user's token.
        """
        lineages = set()  # remove duplicates
        rank = GTDBRank(user_parameters["gtdb_rank"])
        for upa, meta in metadata.items():
            lin = meta.get(_GTDB_LINEAGE_METADATA_KEY)
            if not lin:
                raise errors.MissingLineageError(
                    f"Object {upa} is missing lineage metadata in key {_GTDB_LINEAGE_METADATA_KEY}"
                )
            if lin.startswith(GTDB_UNCLASSIFIED_PREFIX):
                continue  # skip unclassified organisms
            try:
                lin = GTDBLineage(lin, force_complete=False).truncate_to_rank(rank)
            except GTDBLineageError as e:
                raise errors.IllegalLineageError(f"Object {upa} illegal lineage: {str(e)}") from e
            lin_ver = meta.get(_GTDB_LINEAGE_VERSION_METADATA_KEY)
            if not lin_ver:
                raise errors.MissingLineageError(
                    f"Object {upa} is missing lineage version metadata in key "
                    + f"{_GTDB_LINEAGE_VERSION_METADATA_KEY}"
                )
            coll_lin_vers = collection_parameters[_COLLECTION_PARAMS_GTDB_VERSIONS_KEY]
            # May want to do some heuristics here to more fuzzily match, e.g. r207.0 and 207.0
            # should match.
            if lin_ver not in coll_lin_vers:
                raise errors.LineageVersionError(
                    f"Object {upa} lineage version is {lin_ver}, while the collection's allowed versions "
                    + f"are {coll_lin_vers}"
                )
            if lin:
                lineages.add(str(lin))
        return CollectionProcess(
            process=_process_match,
            data_id=internal_match_id,
            args=[lineages, rank != GTDBRank.SPECIES]
        )


MATCHER = GTDBLineageMatcher(
    id="gtdb_lineage",
    description="Matches based on the GTDB lineage string. Requires the GTDB lineage to be "
        + f"in the '{FLD_GENOME_ATTRIBS_GTDB_LINEAGE}' field in the genome attributes data "
        + "product.",
    types=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
    set_types=[
        "KBaseSearch.GenomeSet",  # eventually supposed to be replaced by the KBaseSets version
        "KBaseSets.GenomeSet",
        "KBaseSets.AssemblySet",
    ],
    required_data_products=[genome_attributes.ID],
    user_parameters=GTDBLineageMatcherUserParameters.model_json_schema(),
    collection_parameters=GTDBLineageMatcherCollectionParameters.model_json_schema()
)
