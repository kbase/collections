from conftest import assert_exception_correct
from pytest import raises
from src.common.gtdb_lineage import (
    GTDBLineage,
    GTDBLineageNode,
    GTDBTaxaCount,
    TaxaNodeCount,
    GTDBRank,
    GTDBLineageParseError,
    GTDBLineageRankError,
    GTDBLineageResolutionError,
)

# TODO TEST add more tests

def test_gtdbrank():
    assert GTDBRank.CLASS > GTDBRank.GENUS
    assert GTDBRank.CLASS >= GTDBRank.SPECIES
    assert not GTDBRank.DOMAIN <= GTDBRank.GENUS
    assert not GTDBRank.CLASS < GTDBRank.GENUS
    assert GTDBRank.PHYLUM >= GTDBRank.PHYLUM
    assert GTDBRank.PHYLUM <= GTDBRank.PHYLUM
    
    _bad_comparison(lambda: GTDBRank.FAMILY > 'x', ">", "str")
    _bad_comparison(lambda: GTDBRank.FAMILY >= 1, ">=", "int")
    _bad_comparison(lambda: GTDBRank.FAMILY < [], "<", "list")
    _bad_comparison(lambda: GTDBRank.FAMILY <= 'x', "<=", "str")


def _bad_comparison(callable, op, type_):
    with raises(Exception) as e:
        callable()
    assert_exception_correct(e.value, TypeError(
        f"{op} is not supported between instances of 'GTDBRank' and '{type_}'")
    )


def test_gtdb_lineage():
    linstr = ("d__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales;"
        + "f__DSM-45169;g__Marininema;s__Marininema halotolerans")
    parsed = GTDBLineage(linstr)

    assert parsed.lineage == (
        GTDBLineageNode(GTDBRank.DOMAIN, "Bacteria"),
        GTDBLineageNode(GTDBRank.PHYLUM, "Firmicutes"),
        GTDBLineageNode(GTDBRank.CLASS, "Bacilli"),
        GTDBLineageNode(GTDBRank.ORDER, "Thermoactinomycetales"),
        GTDBLineageNode(GTDBRank.FAMILY, "DSM-45169"),
        GTDBLineageNode(GTDBRank.GENUS, "Marininema"),
        GTDBLineageNode(GTDBRank.SPECIES, "Marininema halotolerans"),
    )


def test_gtdb_lineage_truncated():
    # internal style
    # also test string stripping and trailing semicolons
    parsed = GTDBLineage(
        "d__Bacteria;p__Firmicutes;   c  __  Bacilli  ;o__Thermoactinomycetales;f__DSM-45169   ;")

    assert parsed.lineage == (
        GTDBLineageNode(GTDBRank.DOMAIN, "Bacteria"),
        GTDBLineageNode(GTDBRank.PHYLUM, "Firmicutes"),
        GTDBLineageNode(GTDBRank.CLASS, "Bacilli"),
        GTDBLineageNode(GTDBRank.ORDER, "Thermoactinomycetales"),
        GTDBLineageNode(GTDBRank.FAMILY, "DSM-45169"),
    )

    # gtdb style
    parsed = GTDBLineage("d__Bacteria;p__Firmicutes;c__Bacilli;o__;f__;g__;s__")

    assert parsed.lineage == (
        GTDBLineageNode(GTDBRank.DOMAIN, "Bacteria"),
        GTDBLineageNode(GTDBRank.PHYLUM, "Firmicutes"),
        GTDBLineageNode(GTDBRank.CLASS, "Bacilli"),
    )


def test_gtdb_lineage_str():
    parsed = GTDBLineage("d__Bacteria;p__Firmicutes;c__Bacilli;o__;f__;g__;s__")

    assert str(parsed) == "d__Bacteria;p__Firmicutes;c__Bacilli"


def test_gtdb_lineage_fail():
    _gtdb_lineage_fail("   \t  ", False, GTDBLineageResolutionError(
        "No lineage information in lineage string '   \t  '")
    )
    _gtdb_lineage_fail("d__", False, GTDBLineageResolutionError(
        "No lineage information in lineage string 'd__'")
    )
    _gtdb_lineage_fail("d__domain;p_phylum;c__class", False, GTDBLineageParseError(
        "Invalid lineage node 'p_phylum' in lineage 'd__domain;p_phylum;c__class'")
    )
    _gtdb_lineage_fail(
        "d__Bacteria;p__Firmicutes;c__Bacilli;o__;f__fambly;g__;s__",
        False,
        GTDBLineageParseError("Found resolved rank after unresolved rank in lineage "
            + "string 'd__Bacteria;p__Firmicutes;c__Bacilli;o__;f__fambly;g__;s__'")
    )
    _gtdb_lineage_fail(
        "d__Bacteria;p__Firmicutes;c__Bacilli;x__Thermoactinomycetales;f__DSM-45169",
        False,
        GTDBLineageParseError("Illegal rank in lineage string "
            + "'d__Bacteria;p__Firmicutes;c__Bacilli;x__Thermoactinomycetales;f__DSM-45169': "
            + "No such GTDB rank abbreviation: x")
    )
    _gtdb_lineage_fail(
        "d__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales;"
            + "f__DSM-45169;g__Marininema;s__",
        True,
        GTDBLineageResolutionError(
            "Lineage 'd__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales;f__DSM-45169;"
            + "g__Marininema;s__' does not end with species")
    )
    _gtdb_lineage_fail(
        "d__Bacteria;p__Firmicutes;c__Bacilli;s__Marininema halotolerans;o__Thermoactinomycetales;"
            + "f__DSM-45169;g__Marininema",
        False,
        GTDBLineageRankError(
            "Bad rank order in lineage 'd__Bacteria;p__Firmicutes;c__Bacilli;"
            + "s__Marininema halotolerans;o__Thermoactinomycetales;f__DSM-45169;g__Marininema'"
            )
    )


def _gtdb_lineage_fail(lineage: str, force_complete: bool, expected: Exception):
    with raises(Exception) as got:
        GTDBLineage(lineage, force_complete=force_complete)
    assert_exception_correct(got.value, expected)


def test_gtdb_lineage_truncate_to_rank():
    lin = GTDBLineage(
        "d__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales;"
        + "f__DSM-45169;g__Marininema;s__Marininema halotolerans"
    ).truncate_to_rank(GTDBRank.ORDER)

    assert lin.lineage == (
        GTDBLineageNode(GTDBRank.DOMAIN, "Bacteria"),
        GTDBLineageNode(GTDBRank.PHYLUM, "Firmicutes"),
        GTDBLineageNode(GTDBRank.CLASS, "Bacilli"),
        GTDBLineageNode(GTDBRank.ORDER, "Thermoactinomycetales"),
    )

    lin = GTDBLineage(
        "d__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales"
    ).truncate_to_rank(GTDBRank.FAMILY)

    assert lin is None


def test_taxa_count():
    # very basic test, needs more tests
    tc = GTDBTaxaCount()
    tc.add("d__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales;"
        + "f__DSM-45169;g__Marininema;s__Marininema halotolerans")
    tc.add("d__Bacteria;p__Proteobacteria;c__Alphaproteobacteria;o__Parvibaculales;"
        + "f__Parvibaculaceae;g__Parvibaculum;s__Parvibaculum lavamentivorans")
    got = list(iter(tc))
    expected = [
        TaxaNodeCount(rank=GTDBRank.DOMAIN, name='Bacteria', count=2),
        TaxaNodeCount(rank=GTDBRank.PHYLUM, name='Firmicutes', count=1),
        TaxaNodeCount(rank=GTDBRank.PHYLUM, name='Proteobacteria', count=1),
        TaxaNodeCount(rank=GTDBRank.CLASS, name='Bacilli', count=1),
        TaxaNodeCount(rank=GTDBRank.CLASS, name='Alphaproteobacteria', count=1),
        TaxaNodeCount(rank=GTDBRank.ORDER, name='Thermoactinomycetales', count=1),
        TaxaNodeCount(rank=GTDBRank.ORDER, name='Parvibaculales', count=1),
        TaxaNodeCount(rank=GTDBRank.FAMILY, name='DSM-45169', count=1),
        TaxaNodeCount(rank=GTDBRank.FAMILY, name='Parvibaculaceae', count=1),
        TaxaNodeCount(rank=GTDBRank.GENUS, name='Marininema', count=1),
        TaxaNodeCount(rank=GTDBRank.GENUS, name='Parvibaculum', count=1),
        TaxaNodeCount(rank=GTDBRank.SPECIES, name='Marininema halotolerans', count=1),
        TaxaNodeCount(rank=GTDBRank.SPECIES, name='Parvibaculum lavamentivorans', count=1),
    ]
    assert got == expected
