"""
Models supporting returning a table of key/value pairs, e.g. genome attributes and samples.
"""

from pydantic import BaseModel, Field
import src.common.storage.collection_and_field_names as names
from typing import Any

class AttributeName(BaseModel):
    name: str = Field(
        example=names.FLD_KBASE_ID,
        description="The name of an attribute"
    )


class TableAttributes(BaseModel):
    """
    Attributes for a set of data. Either `fields` and `table` are returned, `data` is
    returned, or `count` is returned.
    The set of available attributes may be different for different collections.
    """
    skip: int = Field(example=0, description="The number of records that were skipped.")
    limit: int = Field(
        example=1000,
        description="The maximum number of results that could be returned. "
            + "0 and meaningless if `count` is specified"
    )
    # may need to return fields with data in the future if we add more info to fields
    fields: list[AttributeName] | None = Field(
        description="The name for each column in the attribute table."
    )
    table: list[list[Any]] | None = Field(
        example=[["my_genome_name"]],
        description="The attributes in an NxM table. Each column's name is available at the "
            + "corresponding index in the fields parameter. Each inner list is a row in the "
            + "table with each entry being the entry for that column."
    )
    data: list[dict[str, Any]] | None = Field(
        example=[{names.FLD_KBASE_ID: "assigned_kbase_id"}],
        description="The attributes as a list of dictionaries."
    )
    count: int | None = Field(
        example=42,
        description="The number of attribute records that match the query.",
    )
