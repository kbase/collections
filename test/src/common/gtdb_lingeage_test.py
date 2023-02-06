from src.common.gtdb_lineage import (
    parse_gtdb_lineage_string,
    GTDBLineageNode,
    GTDBTaxaCount,
    TaxaNodeCount,
)

# TODO TEST add more tests


def test_gtdb_lineage():
    linstr = ("d__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales;"
        + "f__DSM-45169;g__Marininema;s__Marininema halotolerans")
    parsed = parse_gtdb_lineage_string(linstr)

    assert parsed == [
        GTDBLineageNode("d", "Bacteria"),
        GTDBLineageNode("p", "Firmicutes"),
        GTDBLineageNode("c", "Bacilli"),
        GTDBLineageNode("o", "Thermoactinomycetales"),
        GTDBLineageNode("f", "DSM-45169"),
        GTDBLineageNode("g", "Marininema"),
        GTDBLineageNode("s", "Marininema halotolerans"),
    ]


def test_taxa_count():
    # very basic test, needs more tests
    tc = GTDBTaxaCount()
    tc.add("d__Bacteria;p__Firmicutes;c__Bacilli;o__Thermoactinomycetales;"
        + "f__DSM-45169;g__Marininema;s__Marininema halotolerans")
    tc.add("d__Bacteria;p__Proteobacteria;c__Alphaproteobacteria;o__Parvibaculales;"
        + "f__Parvibaculaceae;g__Parvibaculum;s__Parvibaculum lavamentivorans")
    got = list(iter(tc))
    expected = [
        TaxaNodeCount(rank='d', name='Bacteria', count=2),
        TaxaNodeCount(rank='p', name='Firmicutes', count=1),
        TaxaNodeCount(rank='p', name='Proteobacteria', count=1),
        TaxaNodeCount(rank='c', name='Bacilli', count=1),
        TaxaNodeCount(rank='c', name='Alphaproteobacteria', count=1),
        TaxaNodeCount(rank='o', name='Thermoactinomycetales', count=1),
        TaxaNodeCount(rank='o', name='Parvibaculales', count=1),
        TaxaNodeCount(rank='f', name='DSM-45169', count=1),
        TaxaNodeCount(rank='f', name='Parvibaculaceae', count=1),
        TaxaNodeCount(rank='g', name='Marininema', count=1),
        TaxaNodeCount(rank='g', name='Parvibaculum', count=1),
        TaxaNodeCount(rank='s', name='Marininema halotolerans', count=1),
        TaxaNodeCount(rank='s', name='Parvibaculum lavamentivorans', count=1),
    ]
    assert got == expected
