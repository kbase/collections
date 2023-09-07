from pytest import raises

# TODO TEST add more tests, this is just the basics

from src.common.product_models.columnar_attribs_common_models import (
    ColumnType,
    FilterStrategy,
)
from src.service.data_products.filtering import (
    RangeFilter,
    StringFilter,
    SearchQueryPart
)


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
)


def test_rangefilter_from_string():
    for input_, type_, expected in TEST_SET:
        rf = RangeFilter.from_string(type_, None, input_)
        assert rf == expected


def test_rangefilter_to_range_string():
    range_set = (
        (TEST_SET[0][2], "[-1.0,32.0)"),
        (TEST_SET[1][2], "(89.0,"),
        (TEST_SET[2][2], "(3.4,89.1]"),
        (TEST_SET[3][2], ",0.0]"),
        (TEST_SET[4][2], "(2023-09-06T23:59:03+0000,2023-09-06T23:59:21+0000)"),
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
            aql_lines=["IN_RANGE(d.field, @prelow, @prehigh, true, false"],
            bind_vars={"prelow": -1.0, "prehigh": 32.0}
        )),
        (TEST_SET[1][2], SearchQueryPart(
            aql_lines=["d.field > @prelow"], bind_vars={"prelow": 89.0}
        )),
        (TEST_SET[2][2], SearchQueryPart(
            aql_lines=["IN_RANGE(d.field, @prelow, @prehigh, false, true"],
            bind_vars={"prelow": 3.4, "prehigh": 89.1}
        )),
        (TEST_SET[3][2], SearchQueryPart(
            aql_lines=["d.field <= @prehigh"], bind_vars={"prehigh": 0.0}
        )),
        (TEST_SET[4][2], SearchQueryPart(
            aql_lines=["IN_RANGE(d.field, @prelow, @prehigh, false, false"],
            bind_vars={"prelow": "2023-09-06T23:59:03+0000", "prehigh": "2023-09-06T23:59:21+0000"}
        )),
    )
    for rf, expected in aql_set:
        assert rf.to_arangosearch_aql("d.field", "pre", None) == expected


def test_rangefilter_from_string_fail():
    test_set = (
        (ColumnType.INT, "   \t   ", "Missing range information"),
        (
            ColumnType.INT,
            " [  \t   ",
            "Invalid range specification; expected exactly one comma:  \\[  \t   "
        ),
        (
            ColumnType.INT,
            "  [1,2,3]  ",
            "Invalid range specification; expected exactly one comma:   \\[1,2,3\\]  "
        ),
        (ColumnType.INT, "  [[1, 3]  ", "low value is not a number: \\[1"),
        (ColumnType.INT, "  1,3)) ", "high value is not a number: 3\\)"),
        (ColumnType.ENUM, "  1,2", "Invalid type for range filter: enum"),
        (ColumnType.FLOAT, "  foo, 1  ", "low value is not a number: foo"),
        (
            ColumnType.DATE,
            "   2023-09-06T23:59:21+z0000,   ",
            "low value is not an ISO8601 date: 2023\\-09\\-06T23:59:21\\+z0000"
        ),
        (ColumnType.INT, "   (,)   ", "At least one of the low or high values must be provided"),
        (ColumnType.INT, "  (2,1)  ", "The range \\(2.0,1.0\\) excludes all values"),
        (ColumnType.INT, "  (1,1)   ", "The range \\(1.0,1.0\\) excludes all values"),
        (ColumnType.INT, "  [1,1)  ", "The range \\[1.0,1.0\\) excludes all values"),
        (ColumnType.INT, "   (1,1]  ", "The range \\(1.0,1.0\\] excludes all values"),
    )
    for type_, input_, expected in test_set:
        with raises(ValueError, match=f"^{expected}$"):
            RangeFilter.from_string(type_, None, input_)


def test_stringfilter_from_string():
    sf = StringFilter.from_string(None, FilterStrategy.FULL_TEXT, "foo")
    assert sf == StringFilter(FilterStrategy.FULL_TEXT, "foo")


def test_stringfilter_repr():
    sf = eval(repr(StringFilter(FilterStrategy.PREFIX, "bar")))
    assert sf == StringFilter(FilterStrategy.PREFIX, "bar")


def test_stringfilter_to_arangosearch_aql_full_text():
    sf = StringFilter.from_string(None, FilterStrategy.FULL_TEXT, "I'm Mary Poppins y'all")
    assert sf.to_arangosearch_aql("d.somefield", "varpre", "text_rs") == SearchQueryPart(
        variable_assignments={"varpreprefixes": "TOKENS(@varpreinput, \"text_rs\")"},
        aql_lines=["ANALYZER(varpreprefixes ALL == d.somefield, \"text_rs\")"],
        bind_vars={"varpreinput": "I'm Mary Poppins y'all"}
    )


def test_stringfilter_to_arangosearch_aql_prefix():
    sf = StringFilter.from_string(None, FilterStrategy.PREFIX, "rhodo")
    assert sf.to_arangosearch_aql("d.otherfield", "1_", "text_en") == SearchQueryPart(
        variable_assignments={"1_prefixes": "TOKENS(@1_input, \"text_en\")"},
        aql_lines=[
            "ANALYZER(STARTS_WITH(d.otherfield, 1_prefixes, LENGTH(1_prefixes)), \"text_en\")"
        ],
        bind_vars={"1_input": "rhodo"}
    )


def test_stringfilter_from_string_fail():
    test_set=(
        (None, "whee", "strategy is required"),
        (FilterStrategy.FULL_TEXT, None, "string is required and must be non-whitespace only"),
        (FilterStrategy.PREFIX, " \t   ", "string is required and must be non-whitespace only"),
    )
    
    for strategy, input_, expected in test_set:
        with raises(ValueError, match=f"^{expected}$"):
            StringFilter.from_string(None, strategy, input_)
