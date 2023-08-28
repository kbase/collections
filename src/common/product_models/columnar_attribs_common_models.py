"""
Common pydantic models for the data products that consist of tables of attributes; for example
Genome Attributes and Samples.
"""

from dateutil import parser
from enum import Enum

from pydantic import BaseModel, Field, FieldValidationInfo
from pydantic.functional_validators import model_validator, field_validator
from typing import Annotated, Self


class ColumnType(str, Enum):
    """
    The type of a column's values.
    """
    FLOAT = "float",
    """ A float. """
    INT = "int"
    """ An integer. """
    DATE = "date"
    """ An ISO8601 date string. """
    STRING = "string"
    """ A string. """
    ENUM = "enum"
    """ A finite set of strings. """
    
    # TODO FILTERS do we need hidden?


class FilterStrategy(str, Enum):
    """
    The strategy for filtering a column if the column type allows for more than one strategy.
    """
    PREFIX = "prefix"
    """ A string prefix search. """
    FULL_TEXT = "fulltext"
    """ A full text string search. """
    
    # TODO FILTERS substring search


class AttributesColumn(BaseModel):
    """
    Details about a column in an attributes table.
    """
    key: str = Field(
        example="checkm_completeness",
        description="The key, or column name for the column."
    )
    type: ColumnType = Field(
        example=ColumnType.ENUM.value,
        description="The type of the column."
    )
    filter_strategy: Annotated[FilterStrategy | None, Field(
        example=FilterStrategy.PREFIX.value,
        description="The filter strategy for the column if any. Not all column types need "
            + "a filter strategy."
    )] = None
    min_value: Annotated[int | float | str | None, Field(
        example="2023-08-25T22:08:30.576+0000",
        description="The minimum value for the column for numeric and date columns. "
            + "Otherwise null."
    )] = None
    max_value: Annotated[int | float | str | None, Field(
        example="2023-08-25T22:08:33.189+0000",
        description="The maximum value for the column for numeric and date columns. "
            + "Otherwise null."
    )] = None
    enum_values: Annotated[list[str] | None, Field(
        example=["Complete genome", "Chromosome", "Scaffold", "Contig"],
        description="The members of the enumeration for an enum column."
    )] = None
    
    @model_validator(mode="after")
    def _check_filter_strategy(self) -> Self:
        if self.type == ColumnType.STRING:
            if not self.filter_strategy:
                raise ValueError("String types require a filter strategy")
        elif self.filter_strategy:
            raise ValueError("Only string types may have a filter strategy")
        return self
    
    @model_validator(mode="after")
    def _check_enums(self) -> Self:
        if self.type == ColumnType.ENUM and not self.enum_values:
            raise ValueError("Enum columns must specify the enum values")
        return self
    
    @field_validator("min_value", "max_value")
    @classmethod
    def _check_timestamp(cls, v, info: FieldValidationInfo):
        if isinstance(v, str):
            try:
                parser.isoparse(v)
            except ValueError as e:
                raise ValueError(f"{info.field_name} is not a valid ISO8601 date: {v}") from e
        return v


class ColumnarAttributesMeta(BaseModel):
    """
    Metadata about the columns in a table of attributes.
    """
    columns: Annotated[list[AttributesColumn], Field(
        description="The set of columns in the table."
    )]
