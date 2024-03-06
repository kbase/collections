from collections import namedtuple
from typing import Any

from src.common.common_helper import obj_info_to_upa

# TODO: this struct assumes obj_name is the file name,
#  but not true for downloaded fasta file associated with Assembly object
#  This is okay for now as we are currently focusing solely on Genome/Assembly uploads and not a combination of both.
# i.g. assembly object name: 'GCF_000979555.1_gtlEnvA5udCFS_genomic.gbff.gz_assembly'
#      downloaded fasta file name: 'GCF_000979555.1_gtlEnvA5udCFS_genomic.gbff.gz_assembly.fasta'

WSObjTuple = namedtuple(
    "WSObjTuple",
    ["obj_name", "obj_coll_src_dir", "container_internal_file_dir"],
)
"""
WSObjTuple is a named tuple that contains the following fields:
- obj_name: the name of the object (in many cases, also serves as the file name)
- obj_coll_src_dir: the directory of the associated file in the collection source directory
- container_internal_file_dir: the directory of the associated file in the docker container
"""


class UploadResult:
    """
    UploadResult is a class that contains the result of the upload process for a genome or assembly object.

    Attributes:
        - genome_obj_info: the info of the genome object
        - assembly_obj_info: the info of the assembly object
        - genome_tuple: the WSObjTuple of the genome object
        - assembly_tuple: the WSObjTuple of the assembly object
    """

    def __init__(self,
                 assembly_obj_info: list[Any],
                 assembly_tuple: WSObjTuple,
                 genome_obj_info: list[Any] = None,
                 genome_tuple: WSObjTuple = None,
                 ):
        """
        Initializes the UploadResult object.

        Args:
            - assembly_obj_info: the info of the assembly object
            - assembly_tuple: the WSObjTuple of the assembly object
            - genome_obj_info: the info of the genome object
            - genome_tuple: the WSObjTuple of the genome object

        In the case of genome object, all four attributes are required.
        In the case of assembly object, only assembly_obj_info and assembly_tuple are required.
        """

        self._genome_obj_info = genome_obj_info
        self._assembly_obj_info = assembly_obj_info
        self._genome_tuple = genome_tuple
        self._assembly_tuple = assembly_tuple

        if not self._assembly_tuple or not self._assembly_obj_info:
            raise ValueError(
                "Both assembly_obj_info and assembly_tuple must be provided for assembly object"
            )

        if bool(self._genome_tuple) != bool(self._genome_obj_info):
            raise ValueError(
                "Both genome_tuple and genome_obj_info must be provided if one of them is provided"
            )

        self.is_genome = bool(self._genome_tuple) and bool(self._genome_obj_info)

    @property
    def genome_upa(self):
        """
        Returns the UPA of the genome object (in format of wsid_objid_ver)
        """
        return obj_info_to_upa(self._genome_obj_info, underscore_sep=True) if self._genome_obj_info else None

    @property
    def assembly_upa(self):
        """
        Returns the UPA of the assembly object (in format of wsid_objid_ver)
        """
        return obj_info_to_upa(self._assembly_obj_info, underscore_sep=True)

    @property
    def genome_obj_info(self):
        """
        Returns the info of the genome object
        """
        return self._genome_obj_info

    @property
    def assembly_obj_info(self):
        """
        Returns the info of the assembly object
        """
        return self._assembly_obj_info

    @property
    def genome_tuple(self):
        """
        Returns the WSObjTuple of the genome object
        """
        return self._genome_tuple

    @property
    def assembly_tuple(self):
        """
        Returns the WSObjTuple of the assembly object
        """
        return self._assembly_tuple

    def __eq__(self, other) -> bool | NotImplemented:
        """
        Checks if two UploadResult objects are equal.
        """
        if not isinstance(other, UploadResult):
            return NotImplemented

        return [self.genome_obj_info,
                self.assembly_obj_info,
                self.genome_tuple,
                self.assembly_tuple] == [other.genome_obj_info,
                                         other.assembly_obj_info,
                                         other.genome_tuple,
                                         other.assembly_tuple]

    def __repr__(self) -> str:
        return (f"UploadResult(genome_obj_info={self.genome_obj_info}, "
                f"assembly_obj_info={self.assembly_obj_info}, "
                f"genome_tuple={self.genome_tuple}, "
                f"assembly_tuple={self.assembly_tuple})")
