from typing import Callable

from fastapi import Request

from src.common.product_models import columnar_attribs_common_models as col_models
from src.service import errors
from src.service.filtering import analyzers
from src.service.filtering.filters import FilterSet
from src.service.processing import SubsetSpecification

_FILTER_PREFIX = "filter_"


def _get_filter_map(r: Request) -> dict[str, str]:
    """
    Extracts filter specifications from the request query parameters.

    Returns:
    A dictionary mapping field names to filter strings.

    """
    filter_query = {}
    for q in r.query_params.keys():
        if q.startswith(_FILTER_PREFIX):
            field = q[len(_FILTER_PREFIX):]
            if len(r.query_params.getlist(q)) > 1:
                raise errors.IllegalParameterError(
                    f"More than one filter specification provided for field {field}")
            filter_query[field] = r.query_params[q]
    return filter_query


def _append_filters(
        fs: FilterSet,
        filter_query: dict[str, str],
        columns: dict[str, col_models.AttributesColumn],
        trans_field_func: callable = None,
) -> FilterSet:
    """
    Append filters to a FilterSet based on the provided filter query and column definitions.

    fs - The FilterSet to which filters will be appended.
    filter_query - The filter query parameters containing field names and filter strings.
    columns - The columns to which the filters apply, represented as a dictionary
        mapping field names to AttributesColumn objects.
    trans_field_func - A function to transform the field name to valid column name in the filter query
        before applying it to the FilterSet. Default is None.

    """
    for field, querystring in filter_query.items():
        if field not in columns:
            raise errors.IllegalParameterError(f"No such filter field: {field}")
        column = columns[field]
        minlen = analyzers.get_minimum_query_length(column.filter_strategy)
        if minlen and len(querystring) < minlen:
            raise errors.IllegalParameterError(
                f"Filter field '{field}' requires a minimum query length of {minlen}")

        # Translates the query field name to an available column name if necessary.
        # For example, translates query field name '1' to a valid column name 'col_1_val'.
        field = trans_field_func(field) if trans_field_func else field

        fs.append(
            field,
            column.type,
            querystring,
            analyzers.get_analyzer(column.filter_strategy),
            column.filter_strategy
        )
    return fs


async def get_filters(
        r: Request,
        arango_coll: str,
        coll_id: str,
        load_ver: str,
        load_ver_override: bool,
        data_product: str,
        get_columns_func: Callable[[Request, str, str, bool], dict[str, col_models.AttributesColumn]],
        view_name: str = None,
        count: bool = False,
        sort_on: str = None,
        sort_desc: bool = False,
        filter_conjunction: bool = True,
        match_spec: SubsetSpecification = None,
        selection_spec: SubsetSpecification = None,
        keep: dict[str, set[col_models.ColumnType]] = None,
        keep_filter_nulls: bool = False,
        skip: int = 0,
        limit: int = 1000,
        start_after: str = None,
        trans_field_func: Callable[[str], str] = None,
) -> FilterSet:
    """
    Constructs a FilterSet and applies filters using the provided filter query and columns.

    Requires a 'get_columns_func' to generate columns, represented as a dictionary mapping column name/ID to
    columnar_attribs_common_models.AttributesColumn objects.

    r - The request.
    arango_coll - The name of the Arango collection.
    coll_id - The collection ID.
    load_ver - The load version.
    load_ver_override - Whether or not to override the load version.
    data_product - The data product id.
    get_columns_func - A function to generate columns, represented as a dictionary mapping column name/ID to
        columnar_attribs_common_models.AttributesColumn objects.
    view_name - The name of the view to use for filtering.
    count - Whether or not to return the count of matching documents.
    sort_on - The name of the field to sort on.
    sort_desc - Whether or not to sort in descending order.
    filter_conjunction - Whether or not to use a conjunction for filtering.
    match_spec - A subset specification for matching.
    selection_spec - A subset specification for selection.
    keep - A dictionary mapping column names to sets of column types to keep.
    keep_filter_nulls - Whether or not to keep null values when filtering.
    skip - The number of documents to skip.
    limit - The maximum number of documents to return.
    start_after - The value of the field to start after.
    trans_field_func - A function to transform the field name to valid column name in the filter query
    """
    filter_query = _get_filter_map(r)
    if filter_query and not view_name:
        if load_ver_override:
            # If we need this feature than the admin needs to supply the view name to use
            # via the API
            raise ValueError("Filtering is not supported with a load version override.")
        raise ValueError(f"No search view name configured for collection {coll_id}, "
                         + f"data product {data_product}. Cannot perform filtering operation")

    # The caller is responsible for ensuring that each column retrieved through 'get_columns_func'
    # is of type 'columnar_attribs_common_models.AttributesColumn'
    columns = await get_columns_func(r, coll_id, load_ver, load_ver_override)

    if sort_on and sort_on not in columns:
        raise errors.IllegalParameterError(
            f"No such field for collection {coll_id} load version {load_ver}: {sort_on}")
    if keep:
        for col in keep:
            if col not in columns:
                raise errors.IllegalParameterError(
                    f"No such field for collection {coll_id} load version {load_ver}: {col}")
            if keep[col] and columns[col].type not in keep[col]:
                raise errors.IllegalParameterError(
                    f"Column {col} is type '{columns[col].type}', which is not one of the "
                    + f"acceptable types for this operation: {[t.value for t in keep[col]]}")
    fs = FilterSet(
        coll_id,
        load_ver,
        collection=arango_coll,
        view=view_name,
        count=count,
        sort_on=sort_on,
        sort_descending=sort_desc,
        conjunction=filter_conjunction,
        match_spec=match_spec,
        selection_spec=selection_spec,
        keep=list(keep.keys()) if keep else None,
        keep_filter_nulls=keep_filter_nulls,
        skip=skip,
        limit=limit,
        start_after=start_after,
    )
    return _append_filters(fs, filter_query, columns, trans_field_func=trans_field_func)
