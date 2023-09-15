"""
Data structures and methods for parsing, representing, and translating table filters for
data products like genome attributes or samples.
"""

from abc import ABC, abstractmethod
from dateutil import parser
from pydantic import BaseModel, Field
from src.common.product_models.columnar_attribs_common_models import ColumnType, FilterStrategy
from src.common.storage.collection_and_field_names import FLD_LOAD_VERSION

from types import NotImplementedType
from typing import Annotated, Any, Self


_DEFAULT_ANALYZER = "identity"


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
    
    @classmethod
    @abstractmethod
    def from_string(
        cls,
        type_: ColumnType,
        string: str,
        analyzer: str = None,
        strategy: FilterStrategy = None,
    ) -> Self:
        """
        Parse the filter from a filter string. The syntax of the filter string is dependent
        on the filter implementation.
        
        Optional arguments may be required for some filters.
        
        type_ - the column type to which the filter will apply.
        string - the string to parse to create the filter.
        analyzer - the analyzer to use for the filter.
        strategy - the strategy for the filter.
        """
        raise NotImplementedError()

    @abstractmethod
    def to_arangosearch_aql(self, identifier: str, var_prefix: str) -> SearchQueryPart:
        """
        Convert the filter to lines of ArangoSearch AQL and bind variables.
        
        identifier - the identifier for where the search is to take place, for example
            `doc.classification`. This will be inserted verbatim into the search constraint, e.g.
            `f"{identifer} > 47`
        var_prefix - a prefix to apply to variable names, including bind variables,
            to prevent collisions between multiple filters.
        """
        raise NotImplementedError()


def _to_bool_string(b: bool):
    return "true" if b else "false"


def _require_string(s: str, err: str):
    if not s or not s.strip():
        raise ValueError(err)
    return s.strip()


def _gt(num: int, min_: int, name: str):
    if num < min_:
        raise ValueError(f"{name} must be >= {min_}")
    return num


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
            errval = type_ if not type_ else type_.value
            raise ValueError(f"Invalid type for range filter: {errval}")
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
    def from_string(
        cls,
        type_: ColumnType,
        string: str,
        analyzer: str = None,  # @UnusedVariable
        strategy: FilterStrategy = None,  # @UnusedVariable
    ) -> Self:
        """
        Parse the filter from a filter string like [-1, 20).
        
        [ or ] mean the range is inclusive at the low and / or high end, respectively.
        ( or ) (or omitted) mean the range is exclusive.
        An omitted number means no limit on that end of the range; at least one limit is required.
        The separating comma is always required.
        
        The strategy and analyzer arguments are ignored for range filters.
        """
        string = _require_string(string, "Missing range information")
        parts = [x.strip() for x in string.split(",")]
        if len(parts) != 2:
            raise ValueError(f"Invalid range specification; expected exactly one comma: {string}")
        low, low_inclusive = cls._parse_inclusivity(parts[0], True)
        high, high_inclusive = cls._parse_inclusivity(parts[1], False)
        return RangeFilter(type_, low, high, low_inclusive, high_inclusive)

    def to_arangosearch_aql(self, identifier: str, var_prefix: str) -> SearchQueryPart:
        """
        Convert the filter to lines of ArangoSearch AQL and bind variables.
        
        identifier - the identifier for where the search is to take place, for example
            `doc.classification`. This will be inserted verbatim into the search constraint, e.g.
            `f"{identifer} > 47`
        var_prefix - a prefix to apply to variable names, including bind variables,
            to prevent collisions between multiple filters.
        """
        bvpl = f"{var_prefix}low"
        bvph = f"{var_prefix}high"
        if self.low is not None and self.high is not None:
            incllow = _to_bool_string(self.low_inclusive)
            inclhigh = _to_bool_string(self.high_inclusive)
            return SearchQueryPart(
                aql_lines=[f"IN_RANGE({identifier}, @{bvpl}, @{bvph}, {incllow}, {inclhigh})"],
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
    
    def __init__(self, strategy: FilterStrategy, string: str, analyzer: str = None):
        f"""
        Create the string based filter.
        
        strategy - the filter strategy.
        string - the search string.
        analyzer - the analyzer for the filter. If not provided, the {_DEFAULT_ANALYZER} analyzer
            is used.
        """
        if not strategy:
            raise ValueError("strategy is required")
        self.strategy = strategy
        self.string = _require_string(string, "string is required and must be non-whitespace only")
        self.analyzer = (_DEFAULT_ANALYZER if not analyzer or not analyzer.strip()
                         else analyzer.strip())
    
    @classmethod
    def from_string(
            cls,
            type_: ColumnType,  # @UnusedVariable
            string: str,
            analyzer: str = None,
            strategy: FilterStrategy = None,
    ) -> Self:
        """
        Create the filter from a string.
        
        type_ - ignored as there's only one column type for a string filter.
        string - the search string.
        analyzer - the analyzer for the filter. If not provided, the {_DEFAULT_ANALYZER} analyzer
            is used.
        strategy - the filter strategy for the string. Required for string filters.
        """
        return StringFilter(strategy, string, analyzer)

    def __eq__(self, other: object) -> bool | NotImplementedType:
        if not isinstance(other, StringFilter):
            return NotImplemented
        return (self.strategy, self.string, self.analyzer) == (
            other.strategy, other.string, other.analyzer)

    def __repr__(self):
        return (f"StringFilter(FilterStrategy.{self.strategy.name}, \"{self.string}\", "
            + f"\"{self.analyzer}\")")
    
    def to_arangosearch_aql(self, identifier: str, var_prefix: str) -> SearchQueryPart:
        """
        Convert the filter to lines of ArangoSearch AQL and bind variables.
        
        identifier - the identifier for where the search is to take place, for example
            `doc.classification`. This will be inserted verbatim into the search constraint, e.g.
            `f"{identifer} > 47`
        var_prefix - a prefix to apply to variable names, including bind variables,
            to prevent collisions between multiple filters.
        """
        bindvar = f"{var_prefix}input"
        prefixvar = f"{var_prefix}prefixes"
        if self.strategy == FilterStrategy.FULL_TEXT:
            aql_lines=[f"ANALYZER({prefixvar} ALL == {identifier}, \"{self.analyzer}\")"]
        elif self.strategy == FilterStrategy.PREFIX:
            aql_lines=[
                f"ANALYZER(STARTS_WITH({identifier}, {prefixvar}, LENGTH({prefixvar})), "
                    + f"\"{self.analyzer}\")"
            ]
        else:
            # this is impossible to test currently but is here for safety for when we add
            # substring search
            raise ValueError(f"Unexpected filter strategy: {self.strategy}")
        return SearchQueryPart(
            variable_assignments={prefixvar: f"TOKENS(@{bindvar}, \"{self.analyzer}\")"},
            aql_lines=aql_lines,
            bind_vars={bindvar: self.string}
        )


class FilterSet:
    """
    A set of filters that can be translated into ArangoSearch AQL.
    """

    _FILTER_MAP = {
        ColumnType.DATE: RangeFilter,
        ColumnType.INT: RangeFilter,
        ColumnType.FLOAT: RangeFilter,
        ColumnType.STRING: StringFilter,
    }
    
    def __init__(
        self,
        view: str,
        load_ver: str,
        doc_var: str = "doc",
        conjunction: bool = True,
        skip: int = 0,
        limit: int = 1000
    ):
        """
        Create the filter set.
        
        view - the ArangoSearch view to query.
        load_ver - the load version of the data to query.
        doc_var - the variable to use for the ArangoSearch document.
        conjunction - whether to AND (true) or OR (false) the filters together.
        skip - the number of records to skip.
        limit - the maximum number of records to return.
        """
        # this should probably be configurable in the DB, which means added to the loader
        self.view = _require_string(view, "view is required")
        self.load_ver = _require_string(load_ver, "load_ver is required")
        self.doc_var = _require_string(doc_var, "doc_var is required")
        self.conjunction = conjunction
        self.skip = _gt(skip, 0, "skip")
        self.limit = _gt(limit, 1, "limit")
        self._filters = {}

    def __len__(self):
        return len(self._filters)

    def append(
            self,
            field: str,
            type_: ColumnType,
            filter_string: str,
            analyzer: str = None,
            strategy: FilterStrategy = None,
        ) -> Self:
        f"""
        Add a filter to the filter set.
        
        field - the ArangoSearch field upon which the filter will operate.
        type_ - the type of the field.
        filter_string - the filter criteria as represented by a string.
        analyzer - the analyzer for the filter. If not provided the {_DEFAULT_ANALYZER}
            analyzer is used.
        strategy - the filter strategy for columns that possess them.
        
        returns this FilterSet instance for chaining.
        """
        # may want to throw some custom error types from errors.py in this method
        # also maybe in the filters being constructed
        field = _require_string(field, "field is required")
        filter_string = _require_string(
            filter_string, f"filter string is required for field {field}")
        if field in self._filters:
            raise ValueError(f"filter for field {field} was provided more than once")
        filter_ = self._FILTER_MAP.get(type_)
        if not filter_:
            raise ValueError(f"Unsupported column type: {type_}")
        try:
            self._filters[field] = filter_.from_string(type_, filter_string, analyzer, strategy)
        except ValueError as e:
            raise ValueError(f"Invalid filter for field {field}: {str(e)}") from e
        return self
        
    def to_arangosearch_aql(self) -> tuple[str, dict[str, Any]]:
        """
        Generate ArangoSearch AQL and bind vars from the filters.
        """
        var_lines = []
        aql_lines = []
        bind_vars = {
            "@view": self.view,
            "load_ver": self.load_ver,
            "skip": self.skip,
            "limit": self.limit,
        }
        for i, (field, filter_) in enumerate(self._filters.items(), start=1):
            search_part = filter_.to_arangosearch_aql(f"{self.doc_var}.{field}", f"v{i}_")
            if search_part.variable_assignments:
                for var, expression in search_part.variable_assignments.items():
                    var_lines.append(f"LET {var} = {expression}")
            if len(search_part.aql_lines) > 1:
                # no cases use this yet, but just to be safe
                aql_lines.append(["("] + [f"    {l}" for l in search_part.aql_lines] + [")"])
            else:
                aql_lines.append([f"    {search_part.aql_lines[0]}"])
            bind_vars.update(search_part.bind_vars)
        aql = "\n".join(var_lines) + "\n"
        aql += f"FOR {self.doc_var} IN @@view"
        aql += f"\n    SEARCH {self.doc_var}.{FLD_LOAD_VERSION} == @load_ver\n    "
        aql_parts = ["\n        ".join(al) for al in aql_lines]
        op = "AND" if self.conjunction else "OR"
        aql += f"\n        {op}\n    ".join(aql_parts)
        aql += f"\n        LIMIT @skip, @limit\n"
        aql += f"        RETURN {self.doc_var}\n"
        # TODO match, select, sort, count
        return aql, bind_vars
