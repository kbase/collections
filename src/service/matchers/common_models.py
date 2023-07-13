"""
Data structures common to all matchers
"""

from pydantic import BaseModel, Field
from typing import Any
from src.service import models


class Matcher(BaseModel):
    id: str = models.MATCHER_ID_FIELD
    types: list[str] = Field(
        # It'd be nice to specify type versions as well, but KBase has essentially jettisoned
        # that idea
        # It seems weird to hard code this stuff, but this matcher will only work for these
        # specific types, so making it configurable doesn't make a lot of sense
        example=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
        description="The KBase types against which the matcher operates. Ensure that the types "
            + "exist in the workspace service, or errors will occur when attempting to match.",
    )
    set_types: list[str] = Field(
        example=["KBaseSets.GenomeSet", "KBaseSets.AssemblySet"],
        description="The KBase set types against which the matcher operates. Sets will be "
            + "expanded to individual items."
    )
    description: str = Field(
        example="Matches assemblies and genomes via the GTDB lineage",
        description="A free text description of the matcher.",
    )
    required_data_products: list[str] = Field(
        example="genome_attributes",
        description="Any data products that are required for the matcher to function. If the "
            + "collection in which the matcher is installed doesn't specify these data "
            + "products, saving the collection version will fail."
    )
    user_parameters: dict[str, Any] | None = Field(
        example={
            'title': 'LineageMatcherParams',
            'type': 'object',
            'properties': {'lineage_rank': {'title': 'Lineage Rank', 'type': 'string'}},
            'required': ['lineage_rank'],
        },
        description=
            "JSONSchema describing the parameters of the matcher provided by the user, if any",
    )
    collection_parameters: dict[str, Any] | None = Field(
        example={
            'title': 'LineageMatcherParams',
            'type': 'object',
            'properties': {'gtdb_version': {'title': 'Gtdb Version', 'type': 'string'}},
            'required': ['gtdb_version'],
        },
        description="JSONSchema describing parameters provided in the collection document" +
            "when adding a matcher to a collection, if any",
    )
