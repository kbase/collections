from conftest import assert_exception_correct
from pytest import raises
from src.common.gtdb_lineage import (
    parse_gtdb_lineage_string,
    GTDBLineage,
    GTDBLineageNode,
    GTDBTaxaCount,
    TaxaNodeCount,
    GTDBRank,
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
    parsed = parse_gtdb_lineage_string(linstr)

    assert parsed == GTDBLineage([
        GTDBLineageNode(GTDBRank.DOMAIN, "Bacteria"),
        GTDBLineageNode(GTDBRank.PHYLUM, "Firmicutes"),
        GTDBLineageNode(GTDBRank.CLASS, "Bacilli"),
        GTDBLineageNode(GTDBRank.ORDER, "Thermoactinomycetales"),
        GTDBLineageNode(GTDBRank.FAMILY, "DSM-45169"),
        GTDBLineageNode(GTDBRank.GENUS, "Marininema"),
        GTDBLineageNode(GTDBRank.SPECIES, "Marininema halotolerans"),
    ])


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
