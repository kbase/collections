from pytest import raises

# TODO TEST add more tests, this is just the basics

from src.common.product_models.columnar_attribs_common_models import (
    ColumnType
)
from src.service.data_products.filtering import RangeFilter, SearchQueryPart


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

def test_from_string():
    for input_, type_, expected in TEST_SET:
        rf = RangeFilter.from_string(type_, input_)
        assert rf == expected

def test_to_range_string():
    range_set = (
        (TEST_SET[0][2], "[-1.0,32.0)"),
        (TEST_SET[1][2], "(89.0,"),
        (TEST_SET[2][2], "(3.4,89.1]"),
        (TEST_SET[3][2], ",0.0]"),
        (TEST_SET[4][2], "(2023-09-06T23:59:03+0000,2023-09-06T23:59:21+0000)"),
    )
    for rf, expected in range_set:
        assert rf.to_range_string() == expected

def test_repr():
    rf = eval(repr(RangeFilter(ColumnType.FLOAT, -56.1, 1913.1, True, True)))
    assert rf == RangeFilter(ColumnType.FLOAT, -56.1, 1913.1, True, True)
    
    rf = eval(repr(RangeFilter(
        ColumnType.DATE, "2023-09-06T23:57:03+0000", "2023-09-06T23:59:18+0000")))
    assert rf == RangeFilter(
        ColumnType.DATE, "2023-09-06T23:57:03+0000", "2023-09-06T23:59:18+0000")

def test_to_arangosearch_aql():
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
        assert rf.to_arangosearch_aql("d.field", "pre") == expected

def test_from_string_fail():
    test_set = (
        (ColumnType.INT, "   \t   ", "^Missing range information$"),
        (
            ColumnType.INT,
            " [  \t   ",
            "^Invalid range specification; expected exactly one comma:  \\[  \t   $"
        ),
        (
            ColumnType.INT,
            "  [1,2,3]  ",
            "^Invalid range specification; expected exactly one comma:   \\[1,2,3\\]  $"
        ),
        (ColumnType.INT, "  [[1, 3]  ", "^low value is not a number: \\[1$"),
        (ColumnType.INT, "  1,3)) ", "^high value is not a number: 3\\)$"),
        (ColumnType.ENUM, "  1,2", "^Invalid type for range filter: enum$"),
        (ColumnType.FLOAT, "  foo, 1  ", "^low value is not a number: foo$"),
        (
            ColumnType.DATE,
            "   2023-09-06T23:59:21+z0000,   ",
            "^low value is not an ISO8601 date: 2023\\-09\\-06T23:59:21\\+z0000$"
        ),
        (ColumnType.INT, "   (,)   ", "^At least one of the low or high values must be provided$"),
        (ColumnType.INT, "  (2,1)  ", "^The range \\(2.0,1.0\\) excludes all values$"),
        (ColumnType.INT, "  (1,1)   ", "^The range \\(1.0,1.0\\) excludes all values$"),
        (ColumnType.INT, "  [1,1)  ", "^The range \\[1.0,1.0\\) excludes all values$"),
        (ColumnType.INT, "   (1,1]  ", "^The range \\(1.0,1.0\\] excludes all values$"),
    )
    for type_, input_, expected in test_set:
        with raises(ValueError, match=expected):
            RangeFilter.from_string(type_, input_)
    
