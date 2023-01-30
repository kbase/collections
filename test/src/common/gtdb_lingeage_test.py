from src.common.gtdb_lineage import parse_gtdb_lineage_string, GTDBLineageNode

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
