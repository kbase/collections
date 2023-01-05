"""
Matches assemblies and genomes to collections based on the GTDB lineage string.
"""

from pydantic import BaseModel, Field

from src.service.matchers.common_models import Matcher

class LineageMatcherCollectionParameters(BaseModel):
    "Parameters for the GTDB lineage matcher."
    gtdb_version: str = Field(
        example="207.0",
        description="The GTDB version of the collection in which the matcher is installed."
    )


MATCHER = Matcher(
    id="gtdb_lineage",
    description="Matches based on the GTDB lineage string.",
    types=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
    user_parameters=None, # TODO MATCHERS add rank parameter when supporting rank based matching
    collection_parameters=LineageMatcherCollectionParameters.schema()
)