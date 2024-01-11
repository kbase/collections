"""
Common pydantic models for the data products that consist of tables of attributes; for example
Genome Attributes and Samples.
"""

from dateutil import parser
from enum import Enum
from pathlib import Path

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
    BOOL = "bool"
    """ A boolean. """
    
    # TODO FILTERS do we need hidden?


class FilterStrategy(str, Enum):
    """
    The strategy for filtering a column if the column type allows for more than one strategy.
    """
    IDENTITY = "identity"
    """ A string search based on an exact match to the entire string. """
    PREFIX = "prefix"
    """ A string prefix search. """
    FULL_TEXT = "fulltext"
    """ A full text string search. """
    NGRAM = "ngram"
    """ A search based on ngram matching. """


class AttributesColumnSpec(BaseModel):
    """
    A specification for a column in an attributes table.
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
    non_visible: Annotated[bool, Field(
        example=False,
        description="Whether the column is visible to the user. "
             + "If True, the display name and category fields are not required"
    )] = False
    display_name: Annotated[str | None, Field(
        example="Completeness",
        description="The display name of the column. "
            + "Required unless the column is non-visible."
    )] = None
    category: Annotated[str | None, Field(
        example="Quality",
        description="The category of the column."
    )] = None
    description: Annotated[str | None, Field(
        example="The completeness of the genome as determined by CheckM.",
        description="The description of the column."
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
    def _check_visible_col(self) -> Self:
        if not self.non_visible:
            if not self.display_name or not self.category:
                raise ValueError(f"Column {self.key} may not be non-visible and not have a display name or category")
        return self


class ColumnarAttributesSpec(BaseModel):
    """
    Specifications for the columns in a table of attributes.
    """
    columns: Annotated[list[AttributesColumnSpec], Field(
        description="The set of columns in the table."
    )]
    spec_files: Annotated[list[Path], Field(
        description="Paths to the spec files from which the specs where loaded."
    )] = list()


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
        ColumnType.FLOAT: (lambda x: isinstance(x, float) or isinstance(x, int), "a float"),
        ColumnType.DATE: (lambda x: parser.isoparse(str(x)), "a valid ISO8601 date")
    }
    
    @model_validator(mode="after")
    def _check_range(self) -> Self:
        if self.type not in self._RANGE_VALIDATORS.keys():
            return self
        typestr = self.type.value
        for val, coltext in [(self.min_value, "min_value"), (self.max_value, "max_value")]:
            if val is None:
                # The minimum and maximum values may be null because all values in the column are null.
                return self
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
    count: Annotated[int, Field(
        example=51561,
        description="The number of rows in the table."
    )]
