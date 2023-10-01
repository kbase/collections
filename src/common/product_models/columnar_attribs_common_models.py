"""
Common pydantic models for the data products that consist of tables of attributes; for example
Genome Attributes and Samples.
"""

from dateutil import parser
from enum import Enum

from pydantic import BaseModel, Field
from pydantic.functional_validators import model_validator
from typing import Annotated, Self


FIELD_COLUMNS = "columns"


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
    ENUM = "enum"  # There's no actual need for enums yet AFAIK. Will not implement elsewhere
    """ A finite set of strings. """
    
    # TODO FILTERS do we need hidden?


class FilterStrategy(str, Enum):
    """
    The strategy for filtering a column if the column type allows for more than one strategy.
    """
    IDENTITY = "identity"
    """ A string search based on an exact match to the entire string. """
    IN_ARRAY = "inarray"
    """ A string search based on an exact match to one of a set of entries in an array. """
    PREFIX = "prefix"
    """ A string prefix search. """
    FULL_TEXT = "fulltext"
    """ A full text string search. """
    
    # TODO FILTERS substring search


class AttributesColumnSpec(BaseModel):
    """
    A specification for a a column in an attributes table.
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
    
    @model_validator(mode="after")
    def _check_filter_strategy(self) -> Self:
        if self.type == ColumnType.STRING:
            if not self.filter_strategy:
                raise ValueError("String types require a filter strategy")
        elif self.filter_strategy:
            raise ValueError("Only string types may have a filter strategy")
        return self


class ColumnarAttributesSpec(BaseModel):
    """
    Specifications for the columns in a table of attributes.
    """
    columns: Annotated[list[AttributesColumnSpec], Field(
        description="The set of columns in the table."
    )]


class AttributesColumn(AttributesColumnSpec):
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
    def _check_enums(self) -> Self:
        if self.type == ColumnType.ENUM and not self.enum_values:
            raise ValueError("Enum columns must specify the enum values")
        return self
    
    _RANGE_VALIDATORS = {
        ColumnType.INT: (lambda x: isinstance(x, int), "an integer"),
        ColumnType.FLOAT: (lambda x: isinstance(x, float),"a float"),
        ColumnType.DATE: (lambda x: parser.isoparse(str(x)), "a valid ISO8601 date")
    }
    
    @model_validator(mode="after")
    def _check_range(self) -> Self:
        if self.type not in self._RANGE_VALIDATORS.keys():
            return
        typestr = self.type.value
        for val, coltext in [(self.min_value, "min_value"), (self.max_value, "max_value")]:
            if val is None:
                raise ValueError(f"{self.key}: missing {coltext} value for {typestr} column")
            validator, errtext = self._RANGE_VALIDATORS[self.type]
            try:
                if not validator(val):
                    raise ValueError()
            except ValueError as e:
                raise ValueError(f"{self.key}: {coltext} is not {errtext} as required by a "
                                 + f"{typestr} column: {val}") from e
        return self


class ColumnarAttributesMeta(BaseModel):
    """
    Metadata about the columns in a table of attributes.
    """
    columns: Annotated[list[AttributesColumn], Field(
        description="The set of columns in the table."
    )]
