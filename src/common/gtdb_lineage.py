"""
Functions for processing GTDB style lineage information.
"""

from collections import namedtuple

_ABBRV_SPECIES = "s"

GTDB_RANK_ABBREV_TO_FULL_NAME = {
    "d": "domain",
    "p": "phylum",
    "c": "class",
    "o": "order",
    "f": "family",
    "g": "genus",
    _ABBRV_SPECIES: "species",
}


GTDBLineageNode = namedtuple("GTDBLineageNode", "abbreviation name")


def parse_gtdb_lineage_string(linstr: str, force_complete=True) -> list[GTDBLineageNode]:
    """
    Parse a gdtb lineage string into its component parts.
    """
    # This will probably not handle incorrectly formatted lineage strings well. For now all our
    # inputs are expected to be coming from GTDB or GTDB_tk so we'll worry about that later.
    ln = linstr.split(";")
    ret = []
    for lin in ln:
        taxa_abbrev, taxa_name = lin.split("__")
        ret.append(GTDBLineageNode(taxa_abbrev, taxa_name))
    if force_complete and ret[-1].abbreviation != _ABBRV_SPECIES:
        raise ValueError(f"Lineage {linstr} does not end with species")
    return ret
