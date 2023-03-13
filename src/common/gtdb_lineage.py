"""
Functions for processing GTDB style lineage information.
"""

import enum
from collections import namedtuple, defaultdict
from typing import Iterable, Self


class GTDBLineageRankError(Exception):
    """Raised when a GTDB lineage has incorrect ranks."""


class GTDBLineageParseError(Exception):
    """Raised when a GTDB lineage string cannot be parsed."""


class GTDBLineageResolutionError(Exception):
    """Raised with a GTDB lineage is not fully resolved."""


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

    def __repr__(self):
        # doesn't really work as a repr but good enough for debugging. Fix if needed.
        return f"{self.__class__.__name__}({repr(self.rank)}, {self.name})"

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

    def __init__(self, linstr: str, force_complete=False):
        """
        Create the lineage from a gdtb lineage string, omitting unresolved ranks.

        linstr - the lineage string
        force_complete - throw a GTDBResolutionError if the lineage string is not fully resolved.
        """
        # This may probably not handle incorrectly formatted lineage strings well - haven't thought
        # through all possible cases. For now all our inputs are expected to be coming from GTDB
        # or GTDB_tk so we'll worry about that later.
        ret = []
        resolved = True
        linparts = linstr.strip().split(";")
        linparts = linparts if linparts[-1].strip() else linparts[:-1]  # remove trailing ;
        for lin in linparts:
            node = lin.split("__")
            if len(node) != 2:
                raise GTDBLineageParseError(f"Invalid lineage node '{lin}' in lineage '{linstr}'")
            taxa_abbrev, taxa_name = [n.strip() for n in node]
            if not taxa_name:
                resolved = False
            elif not resolved:
                raise GTDBLineageParseError(
                    f"Found resolved rank after unresolved rank in lineage string '{linstr}'")
            else:
                try:
                    ret.append(
                        GTDBLineageNode(GTDBRank.from_abbreviation(taxa_abbrev), taxa_name))
                except ValueError as e:  # maybe want a more specific error?
                    raise GTDBLineageParseError(
                        f"Illegal rank in lineage string '{linstr}': {str(e)}")
        if not ret:
            raise GTDBLineageResolutionError(
                f"No lineage information in lineage string '{linstr}'")
        if force_complete and ret[-1].rank != GTDBRank.SPECIES:
            raise GTDBLineageResolutionError(f"Lineage '{linstr}' does not end with species")
        ranks = [r for r in GTDBRank][:len(ret)]
        for n, r in zip(ret, ranks):
            if n.rank != r:
                raise GTDBLineageRankError(
                    f"Bad rank order in lineage '{linstr}'")
        self.lineage = tuple(ret)

    def __str__(self):
        return ";".join([str(n) for n in self.lineage])

    def __eq__(self, other):
        return self.lineage == other.lineage

    def truncate_to_rank(self, rank: GTDBRank) -> Self | None:
        """
        Truncate this lineage up to and including the given rank. If the rank does not occur in
        the lineage, None is returned.
        """
        try:
            idx = [n.rank for n in self.lineage].index(rank)
            # this is gross and hacky. Need constructor polymorphism to do this sanely
            lin = type(self)("d__domain", force_complete=False)
            lin.lineage = self.lineage[:idx + 1]
            return lin
        except ValueError:
            return None


TaxaNodeCount = namedtuple("TaxaNodeCount", "rank name count")


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
        lineage = GTDBLineage(gtdb_lineage_string)
        for lin in lineage.lineage:
            self._counts[lin.rank][lin.name] += 1

    def __iter__(self) -> Iterable[TaxaNodeCount]:
        for rank in self._counts:
            for name in self._counts[rank]:
                yield TaxaNodeCount(rank, name, self._counts[rank][name])
