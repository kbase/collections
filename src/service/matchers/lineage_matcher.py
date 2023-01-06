"""
Matches assemblies and genomes to collections based on the GTDB lineage string.
"""

from pydantic import BaseModel, Field

from src.service.matchers.common_models import Matcher
from src.common.storage.collection_and_field_names import FLD_GENOME_ATTRIBS_GTDB_LINEAGE

class LineageMatcherCollectionParameters(BaseModel):
    "Parameters for the GTDB lineage matcher."
    gtdb_version: str = Field(
        example="207.0",
        description="The GTDB version of the collection in which the matcher is installed."
    )


MATCHER = Matcher(
    id="gtdb_lineage",
    description="Matches based on the GTDB lineage string. Requires the GTDB lineage to be "
        + f"in the '{FLD_GENOME_ATTRIBS_GTDB_LINEAGE}' field in the genome attributes data "
        + "product.",
    types=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
    required_data_products=["genome_attributes"],
    user_parameters=None, # TODO MATCHERS add rank parameter when supporting rank based matching
    collection_parameters=LineageMatcherCollectionParameters.schema()
)