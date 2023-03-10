"""
Functions for processing GTDB style lineage information.
"""

import enum
from collections import namedtuple, defaultdict
from typing import Iterable, Self


class GTDBLineageRankError(Exception):
    """Raised when a GTDB lineage has incorrect ranks."""


class GTDBRank(str, enum.Enum):
    """
    Ranks in the GTDB lineage system.

    The enum value is the full GTDB rank name (e.g. domain, phylum, etc.)

    Additional instance variables:
    abbrev - the one character abbreviation for the rank used in lineage strings, e.g. d, p, etc.
    """
    abbrev: str
    _order: int

    def __new__(cls, full_rank: str, abbrev: str, order: int):
        # See https://docs.python.org/3/howto/enum.html#when-to-use-new-vs-init
        obj = str.__new__(cls, full_rank)
        obj._value_ = full_rank
        obj.abbrev = abbrev
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
            cls._abbrev_lookup = {r.abbrev: r for r in cls}
        if abbrev not in cls._abbrev_lookup:
            raise ValueError(f"No such GTDB rank abbreviation: {abbrev}")
        return cls._abbrev_lookup[abbrev]


class GTDBLineageNode:
    """
    A node in a GTDB lineage.

    Instance variables:
    rank - the node's rank
    name - the node's scientific name.
    """
    rank: GTDBRank
    name: str

    def __init__(self, rank: GTDBRank, name: str):
        """
        Create the node.

        rank - the node's rank.
        name - the scientific name of the node.
        """
        self.rank = rank
        self.name = name

    def __str__(self):
        return f"{self.rank.abbrev}__{self.name}"

    def __eq__(self, other):
        return (self.rank, self.name) == (other.rank, other.name)


class GTDBLineage:
    """
    A GTDB lineage represeted as a list of lineage nodes. The lineage may be truncated, but must
    have all ranks included up to the truncation point.

    Instance variables:
    lineage - the lineage.
    """

    lineage: tuple[GTDBLineageNode]

    def __init__(self, lineage: list[GTDBLineageNode]):
        """
        Create the lineage.
        """
        if not lineage:
            raise ValueError("lineage required")
        ranks = [r for r in GTDBRank][:len(lineage)]
        for n, r in zip(lineage, ranks):
            if n.rank != r:
                raise GTDBLineageOrderError(
                    f"Bad rank order in lineage {self._to_str(lineage)}")
        self.lineage = tuple(lineage)

    def _to_str(self, lineage: Iterable[GTDBLineageNode]) -> str:
        return ";".join([str(n) for n in lineage])

    def __str__(self):
        return self._to_str(self.lineage)

    def __eq__(self, other):
        return self.lineage == other.lineage

    def truncate_to_rank(self, rank: GTDBRank) -> Self:
        """
        Truncate this lineage up to and including the given rank. If the rank does not occur in
        the lineage, None is returned.
        """
        try:
            idx = [n.rank for n in self.lineage].index(rank)
            return type(self)(self.lineage[:idx + 1])
        except ValueError:
            return None


TaxaNodeCount = namedtuple("TaxaNodeCount", "rank name count")


def parse_gtdb_lineage_string(linstr: str, force_complete=True) -> GTDBLineage:
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
    return GTDBLineage(ret)


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
        for lin in lineage.lineage:
            self._counts[lin.rank][lin.name] += 1

    def __iter__(self) -> Iterable[TaxaNodeCount]:
        for rank in self._counts:
            for name in self._counts[rank]:
                yield TaxaNodeCount(rank, name, self._counts[rank][name])
