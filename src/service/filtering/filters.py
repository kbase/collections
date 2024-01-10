"""
Data structures and methods for parsing, representing, and translating table filters for
data products like genome attributes or samples.
"""

from abc import ABC, abstractmethod
from types import NotImplementedType
from typing import Annotated, Any, Self

from dateutil import parser
from pydantic import BaseModel, Field

from src.common.product_models.columnar_attribs_common_models import ColumnType, FilterStrategy
import src.common.storage.collection_and_field_names as names
from src.service import errors
from src.service.filtering.analyzers import DEFAULT_ANALYZER
from src.service.processing import SubsetSpecification


_PARTICLES_IN_UNIVERSE = 10 ** 80

_TRUE_STR = "true"
_FALSE_STR = "false"


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
    return _TRUE_STR if b else _FALSE_STR


def _require_string(s: str, err: str, optional: bool = False):
    if not s or not s.strip():
        if optional:
            return None
        raise errors.MissingParameterError(err)
    return s.strip()


def _gt(num: int, min_: int, name: str):
    if num < min_:
        raise errors.IllegalParameterError(f"{name} must be >= {min_}")
    return num


class BooleanFilter(AbstractFilter):
    """
    A filter representing a boolean value.
    """

    def __init__(self, bool_value: bool):
        """Initialize the boolean filter with a boolean value.

        bool_value - the boolean value for the filter.
        """
        self.bool_value = bool_value

    def __eq__(self, other: object) -> bool | NotImplementedType:
        """Check equality with another BooleanFilter."""

        if not isinstance(other, BooleanFilter):
            return NotImplemented
        return self.bool_value == other.bool_value

    def __repr__(self) -> str:
        """Represent the BooleanFilter object as a string."""
        return f"BooleanFilter({self.bool_value})"

    @classmethod
    def from_string(
            cls,
            type_: ColumnType,  # @UnusedVariable
            string: str,
            analyzer: str = None,  # @UnusedVariable
            strategy: FilterStrategy = None,  # @UnusedVariable
    ) -> Self:
        """
        Create the filter from a string: "true" or "false".

        string - the search string. Must be either "true" or "false".

        The type_, strategy and analyzer arguments are ignored for the boolean filter.
        """
        string = _require_string(string, "Missing boolean string information")
        string = string.strip().lower()
        if string not in [_TRUE_STR, _FALSE_STR]:
            raise errors.IllegalParameterError(
                f"Invalid boolean specification; expected true or false: {string}")
        return BooleanFilter(string == _TRUE_STR)

    def to_arangosearch_aql(self, identifier: str, var_prefix: str) -> SearchQueryPart:
        """
        Convert the filter to lines of ArangoSearch AQL and bind variables.

        identifier - the identifier for where the search is to take place, for example
            `doc.classification`. This will be inserted verbatim into the search constraint, e.g.
            `f"{identifer} == true`
        var_prefix - a prefix to apply to variable names, including bind variables,
            to prevent collisions between multiple filters.
        """
        bv_key = f"{var_prefix}bool_value"
        return SearchQueryPart(
            aql_lines=[f"{identifier} == @{bv_key}"],
            bind_vars={bv_key: self.bool_value}
        )


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
        self.low = self._parse_val(low, "low range endpoint")
        self.high = self._parse_val(high, "high range endpoint")
        if self.low is None and self.high is None:
            raise errors.IllegalParameterError(
                "At least one of the low or high values for the filter range must be provided")
        self.low_inclusive = low_inclusive
        self.high_inclusive = high_inclusive
        if self.low is not None and self.high is not None and (
            self.low > self.high
            or (self.low == self.high and (not self.low_inclusive or not self.high_inclusive))
        ):
            raise errors.IllegalParameterError(
                f"The filter range {self.to_range_string()} excludes all values")

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
                raise errors.IllegalParameterError(
                    f"{name} value is not an ISO8601 date: {val}") from e
        else:
            try:
                return float(val)
            except ValueError as e:
                raise errors.IllegalParameterError(f"{name} value is not a number: {val}") from e

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
            raise errors.IllegalParameterError(
                f"Invalid range specification; expected exactly one comma: {string}")
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
        analyzer - the analyzer for the filter. If not provided, the {DEFAULT_ANALYZER} analyzer
            is used.
        """
        if not strategy:
            raise ValueError("strategy is required")
        self.strategy = strategy
        self.string = _require_string(
            string, "Filter string is required and must be non-whitespace only")
        self.analyzer = (DEFAULT_ANALYZER if not analyzer or not analyzer.strip()
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
        var_assigns = {prefixvar: f"TOKENS(@{bindvar}, \"{self.analyzer}\")"}
        match self.strategy:
            case FilterStrategy.IDENTITY:
                aql_lines=[f"{identifier} == @{bindvar}"]
                var_assigns = None
            case FilterStrategy.FULL_TEXT:
                aql_lines=[f"ANALYZER({prefixvar} ALL == {identifier}, \"{self.analyzer}\")"]
            case FilterStrategy.PREFIX:
                aql_lines=[
                    f"ANALYZER(STARTS_WITH({identifier}, {prefixvar}, LENGTH({prefixvar})), "
                        + f"\"{self.analyzer}\")"
                ]
            case FilterStrategy.NGRAM:
                # Could make the search fuzzy by reducing the threshold from 1
                # Maybe add a param for it if this is something we're interested in
                # Note there's a possible bug in ngram matching that makes it less suitable
                # for substring matching:
                # https://github.com/arangodb/arangodb/issues/20118
                aql_lines = [f"NGRAM_MATCH({identifier}, @{bindvar}, 1, \"{self.analyzer}\")"]
                var_assigns = None
            case _:
                # this is impossible to test currently but is here for safety
                raise ValueError(f"Unexpected filter strategy: {self.strategy}")
        return SearchQueryPart(
            variable_assignments=var_assigns,
            aql_lines=aql_lines,
            bind_vars={bindvar: self.string}
        )


class FilterSet:
    """
    A set of filters that can be translated into AQL.
    """

    _FILTER_MAP = {
        ColumnType.DATE: RangeFilter,
        ColumnType.INT: RangeFilter,
        ColumnType.FLOAT: RangeFilter,
        ColumnType.STRING: StringFilter,
        ColumnType.BOOL: BooleanFilter,
    }
    
    def __init__(
        self,
        collection_id: str,
        load_ver: str,
        view: str = None,
        collection: str = None,
        count: bool = False,
        start_after: str = None,
        sort_on: str = None,
        sort_descending: bool = False,
        conjunction: bool = True,
        match_spec: SubsetSpecification = SubsetSpecification(),
        selection_spec: SubsetSpecification = SubsetSpecification(),
        skip: int = 0,
        limit: int = 1000,
        keep: list[str] = None,
        keep_filter_nulls: bool = False,
        doc_var: str = "doc",
    ):
        """
        Create the filter set.
        
        collection_id - the ID of the KBase collection to query.
        load_ver - the load version of the data to query.
        view - the ArangoSearch view to query. Required if any filters are added to this filter
            set, as therefore ArangoSearch will be used for the query.
        collection - the Arango collection to query. Required if no filters are added to this
            filter set, as therefore a standard Arango AQL query will be used.
        count - return the total document count rather than the documents. This may cause a
            large view scan.
        sort_on - the field on which to sort, if any.
        sort_descending - sort in the descending direction vs. ascending.
        conjunction - whether to AND (true) or OR (false) the filters together.
        start_after - skip any records prior to and including this value in the `sort_on` field,
            which should contain unique values.
            It is strongly recommended to set up an index that the query can use to skip to
            the correct starting record without a table scan. This parameter allows for
            non-O(n^2) paging of data.
            start_after is not currently implemented for the case where any filters are appended.
        skip - the number of records to skip. Use this parameter wisely, as paging
            through records via increasing skip incrementally is an O(n^2) operation.
        limit - the maximum number of records to return. 0 indicates no limit, which is usually
            a bad idea.
        keep - the fields to return from the database.
        keep_filter_nulls - filter out any documents where any of the keep values are null.
        doc_var - the variable to use for the ArangoSearch document.
        """
        self.collection_id = _require_string(collection_id, "collection_id is required")
        self.load_ver = _require_string(load_ver, "load_ver is required")
        self.view = _require_string(view, "view", True)
        self.collection = _require_string(collection, "collection", True)
        if not self.view and not self.collection:
            raise ValueError("At least one of a view or a collection is required")
        self.count = count
        self.sort_on = _require_string(sort_on, "sort_on", True)
        self.sort_descending = sort_descending
        self.conjunction = conjunction
        self.match_spec = match_spec
        self.selection_spec = selection_spec
        self.start_after = _require_string(start_after, "start_after", True)
        if self.start_after and not self.sort_on:
            raise ValueError("If start_after is supplied sort_on must be supplied")
        self.skip = _gt(skip, 0, "skip")
        self.limit = _gt(limit, 0, "limit")
        self.keep = keep if keep else []
        if any([not bool(x.strip() if x else x) for x in self.keep]):
            raise ValueError("Falsy value in keep")
        self.keep_filter_nulls = keep_filter_nulls
        self.doc_var = _require_string(doc_var, "doc_var is required")
        self._filters = {}

    def __len__(self):
        return len(self._filters)

    def append(
            self,
            field: str,  # currently this is inserted into the aql - use a bind var?
            type_: ColumnType,
            filter_string: str,
            analyzer: str = None,
            strategy: FilterStrategy = None,
        ) -> Self:
        f"""
        Add a filter to the filter set.
        
        field - the ArangoSearch field upon which the filter will operate. It is expected that
            the client has confirmed this is a valid arangosearch field.
        type_ - the type of the field.
        filter_string - the filter criteria as represented by a string.
        analyzer - the analyzer for the filter. If not provided the {DEFAULT_ANALYZER}
            analyzer is used.
        strategy - the filter strategy for columns that possess them.
        
        returns this FilterSet instance for chaining.
        """
        field = _require_string(field, "field is required")
        filter_string = _require_string(
            filter_string,
            f"Filter string is required and must be non-whitespace only for field {field}"
        )
        if field in self._filters:
            raise errors.IllegalParameterError(
                f"Filter for field {field} was provided more than once")
        filter_ = self._FILTER_MAP.get(type_)
        if not filter_:
            raise ValueError(f"Unsupported column type: {type_}")
        try:
            self._filters[field] = filter_.from_string(type_, filter_string, analyzer, strategy)
        except ValueError as e:
            raise ValueError(f"Invalid filter for field {field}: {str(e)}") from e
        except errors.IllegalParameterError as e:
            raise errors.IllegalParameterError(
                f"Invalid filter for field {field}: {str(e)}") from e
        return self
        
    def _process_filters(self):
        var_lines = []
        aql_lines = []
        bind_vars = {
            "@view": self.view,
            "collid": self.collection_id,
            "load_ver": self.load_ver,
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
        return var_lines, aql_lines, bind_vars

    def to_aql(self) -> tuple[str, dict[str, Any]]:
        """
        Generate Arango AQL and bind vars from the filters.
        """
        if self._filters:
            return self._to_arangosearch_aql()
        else:
            return self._to_standard_aql()
        
    def _to_standard_aql(self):
        if not self.collection:
            raise ValueError("If no filters are added to the filter set the collection argument "
                + "is required in the constructor")
        bind_vars = {
            "@collection": self.collection,
            "collid": self.collection_id,
            "load_ver": self.load_ver,
        }
        aql = f"FOR {self.doc_var} IN @@collection\n"
        aql += f"    FILTER {self.doc_var}.{names.FLD_COLLECTION_ID} == @collid\n"
        aql += f"    FILTER {self.doc_var}.{names.FLD_LOAD_VERSION} == @load_ver\n"
        if self.keep_filter_nulls:
            for i, k in enumerate(self.keep):
                aql += f"    FILTER {self.doc_var}.@keep{i} != null\n"
                bind_vars[f"keep{i}"] = k
        matchsel = f"{self.doc_var}.{names.FLD_MATCHES_SELECTIONS}"
        if self.match_spec.get_subset_filtering_id():
            bind_vars["internal_match_id"] = self.match_spec.get_subset_filtering_id()
            aql += f"    FILTER @internal_match_id IN {matchsel}\n"
        if self.selection_spec.get_subset_filtering_id():
            bind_vars["internal_selection_id"] = self.selection_spec.get_subset_filtering_id()
            aql += f"    FILTER @internal_selection_id IN {matchsel}\n"
        if self.count:
            aql += "    COLLECT WITH COUNT INTO length\n"
            aql += "    RETURN length\n"
        else:
            if self.start_after:
                aql += f"    FILTER {self.doc_var}.@sort > @start_after\n"
                bind_vars["start_after"] = self.start_after
            ssl_aql, ssl_bind_vars = self._sort_skip_limit()
            aql += ssl_aql
            bind_vars |= ssl_bind_vars
            if self.keep:
                aql += f"    RETURN KEEP({self.doc_var}, @keep)\n"
                bind_vars["keep"] = self.keep
            else:
                aql += f"    RETURN {self.doc_var}\n"
        return aql, bind_vars

    def _sort_skip_limit(self) -> (str, dict[str, Any]):
        aql = ""
        bind_vars = {}
        if self.sort_on:
            aql += f"    SORT {self.doc_var}.@sort @sortdir\n"
            bind_vars |= {
                "sort": self.sort_on,
                "sortdir": "DESC" if self.sort_descending else "ASC"
            }
        if self.skip or self.limit:
            aql += f"    LIMIT @skip, @limit\n"
            bind_vars |= {
                "skip": self.skip,
                "limit": self.limit if self.limit > 0 else _PARTICLES_IN_UNIVERSE
            }
        return aql, bind_vars

    def _to_arangosearch_aql(self):
        if not self.view:
            raise ValueError("If a filter is added to the filter set the view argument is "
                + "required in the constructor")
        var_lines, aql_lines, bind_vars = self._process_filters()
        aql = ""
        if var_lines:
            aql += "\n".join(var_lines) + "\n"
        if self.count:
            aql += "RETURN COUNT("
        aql += f"FOR {self.doc_var} IN @@view"
        aql += f"\n    SEARCH (\n"
        aql += f"        {self.doc_var}.{names.FLD_COLLECTION_ID} == @collid\n"
        aql += f"        AND\n"
        aql += f"        {self.doc_var}.{names.FLD_LOAD_VERSION} == @load_ver\n"
        if self.keep_filter_nulls:
            for i, k in enumerate(self.keep):
                aql += f"        AND\n"
                aql += f"        {self.doc_var}.@keep{i} != null\n"
                bind_vars[f"keep{i}"] = k
        if self.match_spec.get_subset_filtering_id():
            bind_vars["internal_match_id"] = self.match_spec.get_subset_filtering_id()
            aql += "        AND\n"
            aql += f"        {self.doc_var}.{names.FLD_MATCHES_SELECTIONS} == @internal_match_id\n"
        # this will AND the match and selection. To OR, just OR the two filters 
        if self.selection_spec.get_subset_filtering_id():
            bind_vars["internal_selection_id"] = self.selection_spec.get_subset_filtering_id()
            aql += "        AND\n"
            aql += f"        {self.doc_var}.{names.FLD_MATCHES_SELECTIONS} == "
            aql +=               f"@internal_selection_id\n"
        aql += f"    ) AND (\n    "
        aql_parts = ["\n            ".join(al) for al in aql_lines]
        op = "AND" if self.conjunction else "OR"
        aql += f"\n        {op}\n    ".join(aql_parts)
        aql += f"\n    )\n"
        if not self.count:
            ssl_aql, ssl_bind_vars = self._sort_skip_limit()
            aql += ssl_aql
            bind_vars |= ssl_bind_vars
        if self.keep:
            aql += f"    RETURN KEEP({self.doc_var}, @keep)\n"
            bind_vars["keep"] = self.keep
        else:
            aql += f"    RETURN {self.doc_var}\n"
        # should check if there's a way to speed up counts by returning less stuff or if
        # the query optimizer is smart enough to just do the count and things are fine as is
        if self.count:
            aql += ")\n"
        return aql, bind_vars
