"""
Data structures common to all matchers
"""

from pydantic import BaseModel, Field, validator
from typing import Any


class Matcher(BaseModel):
    id: str = Field(
        example="gtdb_lineage",
        description="Matches assemblies and genomes via the GTDB lineage.",
        regex="^[a-z_]+$",
    )
    types: list[str] = Field(
        # TODO MATCHERS validate types against workspace on startup
        # It'd be nice to specify type versions as well, but KBase has essentially jettisoned
        # that idea
        example=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
        description="The KBase types against which the matcher operates.",
    )
    description: str = Field(
        example="Matches assemblies and genomes via the GTDB lineage",
        description="A free text description of the matcher.",
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
