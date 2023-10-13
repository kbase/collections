from pytest import raises

# TODO TEST add more tests, this is just the basics

import re

from src.common.product_models.columnar_attribs_common_models import (
    ColumnType,
    FilterStrategy,
)
from src.service import errors
from src.service.filtering.filters import (
    RangeFilter,
    StringFilter,
    SearchQueryPart,
    FilterSet,
)
from src.service.processing import SubsetSpecification


TEST_SET = (
    ("[-1, 32)", ColumnType.INT, RangeFilter(ColumnType.INT, -1, 32, True)),
    ("(89,", ColumnType.INT, RangeFilter(ColumnType.INT, 89)),
    ("3.4, 89.1]", ColumnType.FLOAT, RangeFilter(ColumnType.FLOAT, 3.4, 89.1, False, True)),
    (", 0]", ColumnType.FLOAT, RangeFilter(ColumnType.FLOAT, high=0, high_inclusive=True)),
    (
        "(2023-09-06T23:59:03+0000, 2023-09-06T23:59:21+0000",
        ColumnType.DATE,
        RangeFilter(ColumnType.DATE, "2023-09-06T23:59:03+0000", "2023-09-06T23:59:21+0000")
    ),
    ("[1,1]", ColumnType.INT, RangeFilter(ColumnType.INT, 1, 1, True, True)),
)


def test_rangefilter_from_string():
    for input_, type_, expected in TEST_SET:
        rf = RangeFilter.from_string(type_, input_)
        assert rf == expected


def test_rangefilter_to_range_string():
    range_set = (
        (TEST_SET[0][2], "[-1.0,32.0)"),
        (TEST_SET[1][2], "(89.0,"),
        (TEST_SET[2][2], "(3.4,89.1]"),
        (TEST_SET[3][2], ",0.0]"),
        (TEST_SET[4][2], "(2023-09-06T23:59:03+0000,2023-09-06T23:59:21+0000)"),
        (TEST_SET[5][2], "[1.0,1.0]"),
    )
    for rf, expected in range_set:
        assert rf.to_range_string() == expected


def test_rangefilter_repr():
    rf = eval(repr(RangeFilter(ColumnType.FLOAT, -56.1, 1913.1, True, True)))
    assert rf == RangeFilter(ColumnType.FLOAT, -56.1, 1913.1, True, True)
    
    rf = eval(repr(RangeFilter(
        ColumnType.DATE, "2023-09-06T23:57:03+0000", "2023-09-06T23:59:18+0000")))
    assert rf == RangeFilter(
        ColumnType.DATE, "2023-09-06T23:57:03+0000", "2023-09-06T23:59:18+0000")


def test_rangefilter_to_arangosearch_aql():
    # TODO TEST more tests, this isn't all covered
    aql_set = (
        (TEST_SET[0][2], SearchQueryPart(
            aql_lines=["IN_RANGE(d.field, @prelow, @prehigh, true, false)"],
            bind_vars={"prelow": -1.0, "prehigh": 32.0}
        )),
        (TEST_SET[1][2], SearchQueryPart(
            aql_lines=["d.field > @prelow"], bind_vars={"prelow": 89.0}
        )),
        (TEST_SET[2][2], SearchQueryPart(
            aql_lines=["IN_RANGE(d.field, @prelow, @prehigh, false, true)"],
            bind_vars={"prelow": 3.4, "prehigh": 89.1}
        )),
        (TEST_SET[3][2], SearchQueryPart(
            aql_lines=["d.field <= @prehigh"], bind_vars={"prehigh": 0.0}
        )),
        (TEST_SET[4][2], SearchQueryPart(
            aql_lines=["IN_RANGE(d.field, @prelow, @prehigh, false, false)"],
            bind_vars={"prelow": "2023-09-06T23:59:03+0000", "prehigh": "2023-09-06T23:59:21+0000"}
        )),
        (TEST_SET[5][2], SearchQueryPart(
            aql_lines=["IN_RANGE(d.field, @prelow, @prehigh, true, true)"],
            bind_vars={"prelow": 1.0, "prehigh": 1.0}
        )),
    )
    for rf, expected in aql_set:
        assert rf.to_arangosearch_aql("d.field", "pre") == expected


def test_rangefilter_from_string_fail():
    ve = ValueError
    mpe = errors.MissingParameterError
    ipe = errors.IllegalParameterError
    test_set = (
        (ColumnType.INT, "   \t   ", mpe, "Missing range information"),
        (
            ColumnType.INT,
            " [  \t   ",
            ipe,
            "Invalid range specification; expected exactly one comma: ["
        ),
        (
            ColumnType.INT,
            "  [1,2,3]  ",
            ipe,
            "Invalid range specification; expected exactly one comma: [1,2,3]"
        ),
        (ColumnType.INT, "  [[1, 3]  ", ipe, "low range endpoint value is not a number: [1"),
        (ColumnType.INT, "  1,3)) ", ipe, "high range endpoint value is not a number: 3)"),
        (ColumnType.ENUM, "  1,2", ve, "Invalid type for range filter: enum"),
        (None, "  1,2", ve, "Invalid type for range filter: None"),
        (ColumnType.FLOAT, "  foo, 1  ", ipe, "low range endpoint value is not a number: foo"),
        (
            ColumnType.DATE,
            "   2023-09-06T23:59:21+z0000,   ",
            ipe,
            "low range endpoint value is not an ISO8601 date: 2023-09-06T23:59:21+z0000"
        ),
        (ColumnType.INT, "   (,)   ", ipe,
            "At least one of the low or high values for the filter range must be provided"),
        (ColumnType.INT, "  (2,1)  ", ipe, "The filter range (2.0,1.0) excludes all values"),
        (ColumnType.INT, "  (1,1)   ", ipe, "The filter range (1.0,1.0) excludes all values"),
        (ColumnType.INT, "  [1,1)  ", ipe, "The filter range [1.0,1.0) excludes all values"),
        (ColumnType.INT, "   (1,1]  ", ipe, "The filter range (1.0,1.0] excludes all values"),
    )
    for type_, input_, errclass, expected in test_set:
        with raises(errclass, match=f"^{re.escape(expected)}$"):
            RangeFilter.from_string(type_, input_)


def test_stringfilter_from_string():
    sf = StringFilter.from_string(None, "foo", "text_en", FilterStrategy.FULL_TEXT)
    assert sf == StringFilter(FilterStrategy.FULL_TEXT, "foo", "text_en")


def test_stringfilter_repr():
    sf = eval(repr(StringFilter(FilterStrategy.PREFIX, "bar", "trigram")))
    assert sf == StringFilter(FilterStrategy.PREFIX, "bar", "trigram")


def test_stringfilter_to_arangosearch_aql_identity():
    sf = StringFilter.from_string(None, "well dang", None, FilterStrategy.IDENTITY)
    assert sf.to_arangosearch_aql("doc.somefield", "vp") == SearchQueryPart(
        aql_lines=["doc.somefield == @vpinput"],
        bind_vars={"vpinput": "well dang"}
    )


def test_stringfilter_to_arangosearch_aql_in_array():
    sf = StringFilter.from_string(None, "matchidgoeshere", None, FilterStrategy.IN_ARRAY)
    assert sf.to_arangosearch_aql("doc._mtchsel", "pre") == SearchQueryPart(
        aql_lines=["@preinput IN doc._mtchsel"],
        bind_vars={"preinput": "matchidgoeshere"}
    )


def test_stringfilter_to_arangosearch_aql_full_text():
    _stringfilter_to_arangosearch_aql_full_text(None, "identity")
    _stringfilter_to_arangosearch_aql_full_text("    \t   ", "identity")
    _stringfilter_to_arangosearch_aql_full_text("    text_en   ", "text_en")


def _stringfilter_to_arangosearch_aql_full_text(analyzer, expected):
    sf = StringFilter.from_string(
        None, "I'm Mary Poppins y'all", analyzer, FilterStrategy.FULL_TEXT)
    assert sf.to_arangosearch_aql("d.somefield", "varpre") == SearchQueryPart(
        variable_assignments={"varpreprefixes": f"TOKENS(@varpreinput, \"{expected}\")"},
        aql_lines=[f"ANALYZER(varpreprefixes ALL == d.somefield, \"{expected}\")"],
        bind_vars={"varpreinput": "I'm Mary Poppins y'all"}
    )


def test_stringfilter_to_arangosearch_aql_prefix():
    sf = StringFilter.from_string(None, "rhodo", "text_en", FilterStrategy.PREFIX)
    assert sf.to_arangosearch_aql("d.otherfield", "1_") == SearchQueryPart(
        variable_assignments={"1_prefixes": "TOKENS(@1_input, \"text_en\")"},
        aql_lines=[
            "ANALYZER(STARTS_WITH(d.otherfield, 1_prefixes, LENGTH(1_prefixes)), \"text_en\")"
        ],
        bind_vars={"1_input": "rhodo"}
    )


def test_stringfilter_from_string_fail():
    ve = ValueError
    mpe = errors.MissingParameterError
    test_set=(
        (None, "text_en", "whee", ve, "strategy is required"),
        (FilterStrategy.FULL_TEXT, "text_en", None, mpe,
            "Filter string is required and must be non-whitespace only"),
        (FilterStrategy.PREFIX, "text_en", " \t   ", mpe,
            "Filter string is required and must be non-whitespace only"),
    )
    
    for strategy, analyzer, input_, errclass, expected in test_set:
        with raises(errclass, match=f"^{re.escape(expected)}$"):
            StringFilter.from_string(None, input_, analyzer, strategy)


def _filterset_with_defaults_append_filters(fs: FilterSet):
    fs.append("rangefield", ColumnType.INT, "[6,24]"
        ).append("prefixfield", ColumnType.STRING, "foobar", "text_en", FilterStrategy.PREFIX
        ).append("rangefield2", ColumnType.FLOAT, "0.2,"
        ).append("fulltextfield", ColumnType.STRING, "whee", "text_rs", FilterStrategy.FULL_TEXT
        ).append("datefield", ColumnType.DATE, ",2023-09-13T18:51:19+0000]"
        ).append("strident", ColumnType.STRING, "thingy", strategy=FilterStrategy.IDENTITY
        ).append("_mtchsel", ColumnType.STRING, "mtchid", strategy=FilterStrategy.IN_ARRAY
    )
    return fs


def test_filterset_w_defaults():
    fs = _filterset_with_defaults_append_filters(FilterSet("my_search_view", "coll24", "loadver9"))

    aql, bind_vars = fs.to_arangosearch_aql()
    
    assert aql == """
LET v2_prefixes = TOKENS(@v2_input, "text_en")
LET v4_prefixes = TOKENS(@v4_input, "text_rs")
FOR doc IN @@view
    SEARCH (
        doc.coll == @collid
        AND
        doc.load_ver == @load_ver
    ) AND (
        IN_RANGE(doc.rangefield, @v1_low, @v1_high, true, true)
        AND
        ANALYZER(STARTS_WITH(doc.prefixfield, v2_prefixes, LENGTH(v2_prefixes)), "text_en")
        AND
        doc.rangefield2 > @v3_low
        AND
        ANALYZER(v4_prefixes ALL == doc.fulltextfield, "text_rs")
        AND
        doc.datefield <= @v5_high
        AND
        doc.strident == @v6_input
        AND
        @v7_input IN doc._mtchsel
    )
    LIMIT @skip, @limit
    RETURN doc
""".strip() + "\n"
    assert bind_vars == {
        "@view": "my_search_view",
        "collid": "coll24",
        "load_ver": "loadver9",
        "skip": 0,
        "limit": 1000,
        'v1_low': 6.0,
        'v1_high': 24.0,
        'v2_input': 'foobar',
        'v3_low': 0.2,
        "v4_input": "whee",
        "v5_high": "2023-09-13T18:51:19+0000",
        "v6_input": "thingy",
        "v7_input": "mtchid",
    }
    assert len(fs) == 7


def test_filterset_w_defaults_count():
    fs = _filterset_with_defaults_append_filters(
        FilterSet("my_search_view", "coll24", "loadver9", count=True))

    aql, bind_vars = fs.to_arangosearch_aql()
    
    assert aql == """
LET v2_prefixes = TOKENS(@v2_input, "text_en")
LET v4_prefixes = TOKENS(@v4_input, "text_rs")
RETURN COUNT(FOR doc IN @@view
    SEARCH (
        doc.coll == @collid
        AND
        doc.load_ver == @load_ver
    ) AND (
        IN_RANGE(doc.rangefield, @v1_low, @v1_high, true, true)
        AND
        ANALYZER(STARTS_WITH(doc.prefixfield, v2_prefixes, LENGTH(v2_prefixes)), "text_en")
        AND
        doc.rangefield2 > @v3_low
        AND
        ANALYZER(v4_prefixes ALL == doc.fulltextfield, "text_rs")
        AND
        doc.datefield <= @v5_high
        AND
        doc.strident == @v6_input
        AND
        @v7_input IN doc._mtchsel
    )
    RETURN doc
)
""".strip() + "\n"
    assert bind_vars == {
        "@view": "my_search_view",
        "collid": "coll24",
        "load_ver": "loadver9",
        'v1_low': 6.0,
        'v1_high': 24.0,
        'v2_input': 'foobar',
        'v3_low': 0.2,
        "v4_input": "whee",
        "v5_high": "2023-09-13T18:51:19+0000",
        "v6_input": "thingy",
        "v7_input": "mtchid",
    }
    assert len(fs) == 7


def test_filterset_w_all_args():
    fs = FilterSet(
        "my_other_search_view",
        "mycollection",
        "loadver6",
        sort_on="sortfield",
        sort_descending=True,
        conjunction=False,
        match_spec=SubsetSpecification(internal_subset_id="matchy", prefix="m_"),
        selection_spec=SubsetSpecification(internal_subset_id="selly", prefix="s_"),
        skip=24,
        limit=2,
        doc_var="d",
    )
    fs.append("rangefield", ColumnType.INT, "[-2,6]")
    fs.append("prefixfield", ColumnType.STRING, "thingy", "text_en", FilterStrategy.PREFIX)
    aql, bind_vars = fs.to_arangosearch_aql()
    
    assert aql == """
LET v2_prefixes = TOKENS(@v2_input, "text_en")
FOR d IN @@view
    SEARCH (
        d.coll == @collid
        AND
        d.load_ver == @load_ver
        AND
        d._mtchsel == @internal_match_id
        AND
        d._mtchsel == @internal_selection_id
    ) AND (
        IN_RANGE(d.rangefield, @v1_low, @v1_high, true, true)
        OR
        ANALYZER(STARTS_WITH(d.prefixfield, v2_prefixes, LENGTH(v2_prefixes)), "text_en")
    )
    SORT d.@sort @sortdir
    LIMIT @skip, @limit
    RETURN d
""".strip() + "\n"
    assert bind_vars == {
        "@view": "my_other_search_view",
        "collid": "mycollection",
        "load_ver": "loadver6",
        "sort": "sortfield",
        "sortdir":"DESC",
        "internal_match_id": "m_matchy",
        "internal_selection_id": "s_selly",
        "skip": 24,
        "limit": 2,
        'v1_low': -2.0,
        'v1_high': 6.0,
        'v2_input': 'thingy',
    }
    assert len(fs) == 2


def test_filterset_w_all_args_count():
    fs = FilterSet(
        "my_other_search_view",
        "mycollection",
        "loadver6",
        sort_on="sortfield",  # should be ignored
        sort_descending=True,
        count=True,
        conjunction=False,
        match_spec=SubsetSpecification(internal_subset_id="mtc", prefix="w_", mark_only=True),
        selection_spec=SubsetSpecification(internal_subset_id="slc", prefix="5_"),
        skip=24,
        limit=2,
        doc_var="d",
    )
    fs.append("rangefield", ColumnType.INT, "[-2,6]")
    fs.append("prefixfield", ColumnType.STRING, "thingy", "text_en", FilterStrategy.PREFIX)
    aql, bind_vars = fs.to_arangosearch_aql()
    
    assert aql == """
LET v2_prefixes = TOKENS(@v2_input, "text_en")
RETURN COUNT(FOR d IN @@view
    SEARCH (
        d.coll == @collid
        AND
        d.load_ver == @load_ver
        AND
        d._mtchsel == @internal_selection_id
    ) AND (
        IN_RANGE(d.rangefield, @v1_low, @v1_high, true, true)
        OR
        ANALYZER(STARTS_WITH(d.prefixfield, v2_prefixes, LENGTH(v2_prefixes)), "text_en")
    )
    RETURN d
)
""".strip() + "\n"
    assert bind_vars == {
        "@view": "my_other_search_view",
        "collid": "mycollection",
        "load_ver": "loadver6",
        "internal_selection_id": "5_slc",
        'v1_low': -2.0,
        'v1_high': 6.0,
        'v2_input': 'thingy',
    }
    assert len(fs) == 2


def test_filterset_sort_asc():
    fs = FilterSet(
        "sorty_sort",
        "sortcol",
        "loadver89",
        sort_on="somefield",  # should be ignored
    )
    fs.append("rangefield", ColumnType.INT, "[-2,6]")
    aql, bind_vars = fs.to_arangosearch_aql()
    assert aql == """
FOR doc IN @@view
    SEARCH (
        doc.coll == @collid
        AND
        doc.load_ver == @load_ver
    ) AND (
        IN_RANGE(doc.rangefield, @v1_low, @v1_high, true, true)
    )
    SORT doc.@sort @sortdir
    LIMIT @skip, @limit
    RETURN doc
""".strip() + "\n"
    assert bind_vars == {
        "@view": "sorty_sort",
        "collid": "sortcol",
        "load_ver": "loadver89",
        "sort": "somefield",
        "sortdir": "ASC",
        "skip": 0,
        "limit": 1000,
        'v1_low': -2.0,
        'v1_high': 6.0,
    }
    assert len(fs) == 1


def test_filterset_w_1_filter():
    fs = FilterSet(
        "so_many_search_views",
        "PMI",
        "loadyload",
    )
    fs.append("shoe_size", ColumnType.FLOAT, "[-56.9, 32.1)")
    aql, bind_vars = fs.to_arangosearch_aql()
    
    assert aql == """
FOR doc IN @@view
    SEARCH (
        doc.coll == @collid
        AND
        doc.load_ver == @load_ver
    ) AND (
        IN_RANGE(doc.shoe_size, @v1_low, @v1_high, true, false)
    )
    LIMIT @skip, @limit
    RETURN doc
""".strip() + "\n"
    assert bind_vars == {
        "@view": "so_many_search_views",
        "collid": "PMI",
        "load_ver": "loadyload",
        "skip": 0,
        "limit": 1000,
        'v1_low': -56.9,
        'v1_high': 32.1,
    }
    assert len(fs) == 1


def test_filterset_len_0():
    assert len(FilterSet("v", "c", "lv")) == 0


def test_filterset_fail_construct():
    m = errors.MissingParameterError
    i = errors.IllegalParameterError
    _filterset_fail_construct(None, "c", "lv", 0, 1, "d", m, "view is required")
    _filterset_fail_construct("   \t  ", "c", "lv", 0, 1, "d", m, "view is required")
    _filterset_fail_construct("v", None, "lv", 0, 1, "d", m, "collection_id is required")
    _filterset_fail_construct("v", "    \t   ", "lv", 0, 1, "d", m, "collection_id is required")
    _filterset_fail_construct("v", "c", None, 0, 1, "d", m, "load_ver is required")
    _filterset_fail_construct("v", "c", "  \t  ", 0, 1, "d", m, "load_ver is required")
    _filterset_fail_construct("v", "c", "lv", 0, 1, None, m, "doc_var is required")
    _filterset_fail_construct("v", "c", "lv", 0, 1, "   \t   ", m, "doc_var is required")
    _filterset_fail_construct("v", "c", "lv", -1, 1, "d", i, "skip must be >= 0")
    _filterset_fail_construct("v", "c", "lv", 1, 0, "d", i, "limit must be >= 1")


def _filterset_fail_construct(
        view: str,
        coll_id: str,
        load_ver: str,
        skip: int,
        limit: int,
        doc_var: str,
        errclass: Exception,
        expected: str
    ):
    with raises(errclass, match=f"^{re.escape(expected)}$"):
        FilterSet(view, coll_id, load_ver, skip=skip, limit=limit, doc_var=doc_var)


def test_filterset_fail_append():
    c = ColumnType.INT
    v = ValueError
    m = errors.MissingParameterError
    i = errors.IllegalParameterError
    _filterset_fail_append(None, c, "s", None, m, "field is required")
    _filterset_fail_append("   \t   ", c, "s", None, m, "field is required")
    _filterset_fail_append("f", None, "s", None, v, "Unsupported column type: None")
    _filterset_fail_append("f", c, None, None, m,
        "Filter string is required and must be non-whitespace only for field f")
    _filterset_fail_append("f", c, "  \n  ", None, m,
        "Filter string is required and must be non-whitespace only for field f")
    _filterset_fail_append(
        "f", c, "whee", None, i,
        "Invalid filter for field f: Invalid range specification; expected exactly one comma: whee"
    )
    _filterset_fail_append(
        "f", ColumnType.STRING, "f", None, v,
        "Invalid filter for field f: strategy is required"
    )


def _filterset_fail_append(
        field: str,
        coltype: ColumnType,
        string: str,
        strategy: FilterStrategy,
        errclass: Exception,
        expected: str
    ):
    with raises(errclass, match=f"^{re.escape(expected)}$"):
        FilterSet("v", "c", "lv").append(field, coltype, string, None, strategy)


def test_filterset_fail_append_duplicate_field():
    expected = "Filter for field myfield was provided more than once"
    with raises(errors.IllegalParameterError, match=f"^{re.escape(expected)}$"):
        FilterSet("v", "c", "lv"
            ).append("myfield", ColumnType.INT, "8,"
            ).append("myfield", ColumnType.STRING, "foo", "text_en", FilterStrategy.FULL_TEXT)


def test_filterset_fail_to_arangosearch_aql():
    expected = "At least one filter is required"
    with raises(ValueError, match=f"^{re.escape(expected)}$"):
        FilterSet("v", "c", "lv").to_arangosearch_aql()
