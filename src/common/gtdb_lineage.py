"""
Functions for processing GTDB style lineage information.
"""

import enum
from collections import namedtuple, defaultdict
from typing import Iterable


class GTDBRank(str, enum.Enum):
    """
    Ranks in the GTDB lineage system.
    """
    rank_abbrev: str
    _order: int

    def __new__(cls, full_rank: str, rank_abbrev: str, order: int):
        # See https://docs.python.org/3/howto/enum.html#when-to-use-new-vs-init
        obj = str.__new__(cls, full_rank)
        obj._value_ = full_rank
        obj.rank_abbrev = rank_abbrev
        obj._order = order
        return obj
    
    DOMAIN =  ("domain", "d", 7)
    PHYLUM =  ("phylum", "p", 6)
    CLASS =   ("class", "c", 5)
    ORDER =   ("order", "o", 4)
    FAMILY =  ("family", "f", 3)
    GENUS =   ("genus", "g", 2)
    SPECIES = ("species", "s", 1)

    def _err(self, op, other):
        raise TypeError(f"{op} is not supported between instances of '{self.__class__.__name__}' "
            + f"and '{other.__class__.__name__}'")

    def __lt__(self, other):
        if other.__class__ is not GTDBRank:
            self._err("<", other)
        return self._order < other._order

    def __le__(self, other):
        if other.__class__ is not GTDBRank:
            self._err("<=", other)
        return self._order <= other._order

    def __gt__(self, other):
        if other.__class__ is not GTDBRank:
            self._err(">", other)
        return self._order > other._order

    def __ge__(self, other):
        if other.__class__ is not GTDBRank:
            self._err(">=", other)
        return self._order >= other._order

    @classmethod
    def from_abbreviation(cls, abbrev: str):
        if not hasattr(cls, "_abbrev_lookup"):
            cls._abbrev_lookup = {r.rank_abbrev: r for r in cls}
        if abbrev not in cls._abbrev_lookup:
            raise ValueError(f"No such GTDB rank abbreviation: {abbrev}")
        return cls._abbrev_lookup[abbrev]


GTDBLineageNode = namedtuple("GTDBLineageNode", "rank name")


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
        ret.append(GTDBLineageNode(GTDBRank.from_abbreviation(taxa_abbrev), taxa_name))
    if force_complete and ret[-1].rank != GTDBRank.SPECIES:
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
            self._counts[lin.rank][lin.name] += 1

    def __iter__(self) -> Iterable[TaxaNodeCount]:
        for rank in self._counts:
            for name in self._counts[rank]:
                yield TaxaNodeCount(rank, name, self._counts[rank][name])
