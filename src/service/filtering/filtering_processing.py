from typing import Callable

from fastapi import Request

from src.common.product_models import columnar_attribs_common_models as col_models
from src.service import errors, app_state
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
        columns: list[col_models.AttributesColumn],
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
    # TODO CODE: 6 required args is getting pretty long. We may want to consider a builder
    """
    Constructs a FilterSet and applies filters using the provided filter query and columns.

    Requires a 'get_columns_func' to generate columns, represented as a dictionary mapping column name/ID to
    columnar_attribs_common_models.AttributesColumn objects.

    r - The request.
    arango_coll - The name of the Arango collection to filter on.
    coll_id - The KBase Collection ID for the Collection that will be filtered.
    load_ver - The load version of the KBase Collection.
    load_ver_override - Whether or not the load version was overridden by a collections
        admin.
    data_product - The ID of the data product being filtered.
    columns - the column definitions for the data product and KBase Collection.
    view_name - The name of the ArangoSearch view to use for filtering, if any.
        A view must be supplied if any filter query parameters are passed in the request.
    count - Whether or not to return the count of matching documents.
    sort_on - The name of the field to sort on.
    sort_desc - Whether or not to sort in descending order.
    filter_conjunction - use a conjunction rather than disjunction when applying multiple filters.
    match_spec - A subset specification for matching.
    selection_spec - A subset specification for selection.
    keep - A dictionary mapping column names to keep in the returned data set to
        allowable column types for the column names.
    keep_filter_nulls - Whether or not to keep null values when filtering.
    skip - The number of documents to skip.
    limit - The maximum number of documents to return.
    start_after - The value of the field to start after.
    trans_field_func - A function to transform the field name to valid column name in the filter query
    """
    appstate = app_state.get_app_state(r)
    filter_query = _get_filter_map(r)
    if filter_query:
        if load_ver_override:
            # If we need this feature than the admin needs to supply the view name to use
            # via the API
            raise ValueError("Filtering is not supported with a load version override.")

        if view_name:
            if not await appstate.arangostorage.has_search_view(view_name):
                raise ValueError(f"View {view_name} does not exist for collection {coll_id}")
        else:
            raise ValueError(f"No search view name configured for collection {coll_id}, "
                             + f"data product {data_product}. Cannot perform filtering operation")

    columns = {c.key: c for c in columns}

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
