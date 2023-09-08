"""
Data structures and methods for parsing, representing, and translating table filters for
data products like genome attributes or samples.
"""

from abc import ABC, abstractmethod
from dateutil import parser
from pydantic import BaseModel, Field
from src.common.product_models.columnar_attribs_common_models import ColumnType, FilterStrategy

from types import NotImplementedType
from typing import Annotated, Any, Self


class SearchQueryPart(BaseModel):
    variable_assignments: Annotated[dict[str, str] | None, Field(
        description="A mapping of variable name to AQL expression. The variables must be assigned "
            + "prior to the SEARCH operation. The variable names are expected to be unique "
            + "across all filters.")
    ] = None
    aql_lines: list[str] = Field(  # TDOO better docs once I figure out integration
        description="One or more lines of ArangoSearch AQL representing the filter")
    bind_vars: dict[str, Any] = Field(
        description="The bind variables for the AQL lines. They are expected to be unique "
            + "across all filters")


class AbstractFilter(ABC):
    """
    The abstract base class for all filters.
    """
    
    @abstractmethod
    def from_string(self, type_: ColumnType, strategy: FilterStrategy, string: str) -> Self:
        """
        Parse the filter from a filter string. The syntax of the filter string is dependent
        on the filter implementation.
        
        type_ - the column type to which the filter will apply.
        strategy - the strategy for the filter.
        string - the string to parse to create the filter.
        """
        raise NotImplementedError()

    @abstractmethod
    def to_arangosearch_aql(self, identifier: str, var_prefix: str, analyzer: str
    ) -> SearchQueryPart:
        """
        Convert the filter to lines of ArangoSearch AQL and bind variables.
        
        identifier - the identifier for where the search is to take place, for example
            `doc.classification`. This will be inserted verbatim into the search constraint, e.g.
            `f"{identifer} > 47`
        var_prefix - a prefix to apply to variable names, including bind variables,
            to prevent collisions between multiple filters.
        analyzer - the analyzer to use for the search.
        """
        raise NotImplementedError()


def _to_bool_string(b: bool):
    return "true" if b else "false"


class RangeFilter(AbstractFilter):
    """
    A filter representing a range of numbers or dates.
    """
    
    def __init__(
        self,
        type_: ColumnType,
        low: float | int | str | None = None,
        high: float | int | str | None = None,
        low_inclusive: bool = False,
        high_inclusive: bool = False
    ):
        """
        Create a range filter for numbers or ISO8601 dates.
        
        At least one of low or high must be supplied.
        
        type - the type of the range. Only ints, floats, and dates are allowed.
        low - the low end of the range.
        high - the high end of the range.
        low_inclusive - whether to include the low value in the range.
        high_inclusive - whether to include the high value in the range.
        """
        if type_ not in [ColumnType.DATE, ColumnType.INT, ColumnType.FLOAT]:
            raise ValueError(f"Invalid type for range filter: {type_.value}")
        self.type = type_
        self.low = self._parse_val(low, "low")
        self.high = self._parse_val(high, "high")
        if self.low is None and self.high is None:
            raise ValueError("At least one of the low or high values must be provided")
        self.low_inclusive = low_inclusive
        self.high_inclusive = high_inclusive
        if self.low is not None and self.high is not None and (
            self.low > self.high
            or (self.low == self.high and (not self.low_inclusive or not self.high_inclusive))
        ):
            raise ValueError(f"The range {self.to_range_string()} excludes all values")

    def to_range_string(self):
        """
        Create a range string from this instance. See `from_string` for a description of the
        format.
        """
        ret = ""
        if self.low is not None:
            ret += "[" if self.low_inclusive else "("
            ret += str(self.low)
        ret += ","
        if self.high is not None:
            ret += str(self.high)
            ret += "]" if self.high_inclusive else ")"
        return ret

    @classmethod
    def _parse_inclusivity(cls, part, start):
        inclusive = False
        if start:
            if part.startswith("["):
                inclusive = True
            if part.startswith("(") or part.startswith("["):
                part = part[1:]
        else:
            if part.endswith("]"):
                inclusive = True
            if part.endswith(")") or part.endswith("]"):
                part = part[:-1]
        return part, inclusive
    
    def _parse_val(self, val, name):
        if val is None or val == "":
            return None
        if self.type == ColumnType.DATE:
            try:
                parser.isoparse(val)
                return val
            except ValueError as e:
                raise ValueError(f"{name} value is not an ISO8601 date: {val}")
        else:
            try:
                return float(val)
            except ValueError as e:
                raise ValueError(f"{name} value is not a number: {val}") from e

    def __eq__(self, other: object) -> bool | NotImplementedType:
        if not isinstance(other, RangeFilter):
            return NotImplemented
        return (self.type, self.low, self.high, self.low_inclusive, self.high_inclusive) == (
            other.type, other.low, other.high, other.low_inclusive, other.high_inclusive)
        
    def __repr__(self):
        l, h = self.low, self.high
        if self.type == ColumnType.DATE:
            l = f'"{l}"'
            h = f'"{h}"'
        return (f"RangeFilter(ColumnType.{self.type.name}, {l}, {h}, "
            + f"{self.low_inclusive}, {self.high_inclusive})")

    @classmethod
    def from_string(cls, type_: ColumnType, strategy: FilterStrategy, string: str) -> Self:
        """
        Parse the filter from a filter string like [-1, 20).
        
        [ or ] mean the range is inclusive at the low and / or high end, respectively.
        ( or ) (or omitted) mean the range is exclusive.
        An omitted number means no limit on that end of the range; at least one limit is required.
        The separating comma is always required.
        
        The strategy argument is ignored for range filters.
        """
        if not string.strip():
            raise ValueError("Missing range information")
        parts = [x.strip() for x in string.split(",")]
        if len(parts) != 2:
            raise ValueError(f"Invalid range specification; expected exactly one comma: {string}")
        low, low_inclusive = cls._parse_inclusivity(parts[0], True)
        high, high_inclusive = cls._parse_inclusivity(parts[1], False)
        return RangeFilter(type_, low, high, low_inclusive, high_inclusive)

    def to_arangosearch_aql(self, identifier: str, var_prefix: str, analyzer: str
    ) -> SearchQueryPart:
        """
        Convert the filter to lines of ArangoSearch AQL and bind variables.
        
        identifier - the identifier for where the search is to take place, for example
            `doc.classification`. This will be inserted verbatim into the search constraint, e.g.
            `f"{identifer} > 47`
        var_prefix - a prefix to apply to variable names, including bind variables,
            to prevent collisions between multiple filters.
        analyzer - unused for this filter.
        """
        bvpl = f"{var_prefix}low"
        bvph = f"{var_prefix}high"
        if self.low is not None and self.high is not None:
            incllow = _to_bool_string(self.low_inclusive)
            inclhigh = _to_bool_string(self.high_inclusive)
            return SearchQueryPart(
                aql_lines=[f"IN_RANGE({identifier}, @{bvpl}, @{bvph}, {incllow}, {inclhigh}"],
                bind_vars={bvpl: self.low, bvph: self.high}
            )
        if self.low is not None:
            gt = ">=" if self.low_inclusive else ">"
            return SearchQueryPart(
                aql_lines=[f"{identifier} {gt} @{bvpl}"], bind_vars={bvpl: self.low}
            )
        if self.high is not None:
            lt = "<=" if self.high_inclusive else "<"
            return SearchQueryPart(
                aql_lines=[f"{identifier} {lt} @{bvph}"], bind_vars={bvph: self.high}
            )


class StringFilter(AbstractFilter):
    """
    A filter representing a string based filter, e.g. an exact match to a token, token prefix,
    etc.
    """
    
    def __init__(self, strategy: FilterStrategy, string: str):
        """
        Create the string based filter.
        
        strategy - the filter strategy.
        string - the search string.
        """
        if not strategy:
            raise ValueError("strategy is required")
        self.strategy = strategy
        if not string or not string.strip():
            raise ValueError("string is required and must be non-whitespace only")
        self.string = string
    
    @classmethod
    def from_string(cls, type_: ColumnType, strategy: FilterStrategy, string: str) -> Self:
        """
        Create the filter from a string.
        
        type_ - ignored as there's only one column type for a string filter.
        strategy - the filter strategy for the string.
        string - the search string.
        """
        return StringFilter(strategy, string)

    def __eq__(self, other: object) -> bool | NotImplementedType:
        if not isinstance(other, StringFilter):
            return NotImplemented
        return (self.strategy, self.string) == (other.strategy, other.string)

    def __repr__(self):
        return f"StringFilter(FilterStrategy.{self.strategy.name}, \"{self.string}\")"
    
    def to_arangosearch_aql(self, identifier: str, var_prefix: str, analyzer: str
    ) -> SearchQueryPart:
        """
        Convert the filter to lines of ArangoSearch AQL and bind variables.
        
        identifier - the identifier for where the search is to take place, for example
            `doc.classification`. This will be inserted verbatim into the search constraint, e.g.
            `f"{identifer} > 47`
        var_prefix - a prefix to apply to variable names, including bind variables,
            to prevent collisions between multiple filters.
        analyzer - the analyzer to use for the search.
        """
        bindvar = f"{var_prefix}input"
        prefixvar = f"{var_prefix}prefixes"
        if self.strategy == FilterStrategy.FULL_TEXT:
            aql_lines=[f"ANALYZER({prefixvar} ALL == {identifier}, \"{analyzer}\")"]
        elif self.strategy == FilterStrategy.PREFIX:
            aql_lines=[
                f"ANALYZER(STARTS_WITH({identifier}, {prefixvar}, LENGTH({prefixvar})), "
                    + f"\"{analyzer}\")"
            ]
        else:
            # this is impossible to test currently but is here for safety for when we add
            # substring search
            raise ValueError(f"Unexpected filter strategy: {self.strategy}")
        return SearchQueryPart(
            variable_assignments={prefixvar: f"TOKENS(@{bindvar}, \"{analyzer}\")"},
            aql_lines=aql_lines,
            bind_vars={bindvar: self.string}
        )
