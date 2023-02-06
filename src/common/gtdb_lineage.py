"""
Functions for processing GTDB style lineage information.
"""

from collections import namedtuple, defaultdict
from typing import Iterable

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


TaxaNodeCount = namedtuple("TaxaNodeCount", "rank name count")


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


class GTDBTaxaCount:
    """
    Counts GTDB taxa by rank and name. Implements the Iterable interface.
    """

    def __init__(self):
        """
        Create the counter.
        """
        self._counts = defaultdict(lambda: defaultdict(int))

    def add(self, gtdb_lineage_string: str) -> None:
        """
        Process a GTDB lineage string and add it to the taxa count information.
        """
        lineage = parse_gtdb_lineage_string(gtdb_lineage_string)
        for lin in lineage:
            self._counts[lin.abbreviation][lin.name] += 1

    def __iter__(self) -> Iterable[TaxaNodeCount]:
        for rank in self._counts:
            for name in self._counts[rank]:
                yield TaxaNodeCount(rank, name, self._counts[rank][name])
