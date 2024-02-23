from collections import namedtuple
from typing import Any

# TODO: this struct assumes obj_name is the file name,
#  but not true for downloaded fasta file associated with Assembly object
#  This is okay for now as we are currently focusing solely on Genome/Assembly uploads and not a combination of both.
# i.g. assembly object name: 'GCF_000979555.1_gtlEnvA5udCFS_genomic.gbff.gz_assembly'
#      downloaded fasta file name: 'GCF_000979555.1_gtlEnvA5udCFS_genomic.gbff.gz_assembly.fasta'
"""
WSObjTuple is a named tuple that contains the following fields:
- obj_name: the name of the object (in many cases, also serves as the file name)
- host_file_dir: the directory of the associated file in the source collection directory
- container_internal_file_dir: the directory of the associated file in the container
"""
WSObjTuple = namedtuple(
    "WSObjTuple",
    ["obj_name", "host_file_dir", "container_internal_file_dir"],
)


class UploadResult:
    """
    UploadResult is a class that contains the result of the upload process for a genome or assembly object.

    Attributes:
    - genome_upa: the UPA of the genome object (in format of wsid_objid_ver)
    - assembly_upa: the UPA of the assembly object (in format of wsid_objid_ver)
    - genome_obj_info: the info of the genome object
    - assembly_obj_info: the info of the assembly object
    - genome_tuple: the WSObjTuple of the genome object
    - assembly_tuple: the WSObjTuple of the assembly object

    In the case of an genome object result, all of the attributes are required.
    In the case of an assembly object result, only assembly_upa and assembly_tuple are required.
    """

    def _validate_upa(self) -> None:

        # The presence of the assembly object is required for both genome and assembly cases
        if not self._assembly_upa:
            raise ValueError("assembly_upa is required")

        # UPA format: wsid_objid_ver
        for upa in (self._genome_upa, self._assembly_upa):
            if not upa:
                # in Assembly only scenario, genome_upa is not required
                continue
            try:
                map(int, upa.split('_'))
            except ValueError:
                raise ValueError(f"Invalid UPA format: {upa}")

    def _check_is_genome(self) -> bool:

        if bool(self._genome_tuple) != bool(self._genome_upa):  # xor
            raise ValueError(
                "Both genome_tuple and genome_upa must be provided if one of them is provided"
            )

        is_genome = bool(self._genome_upa) and bool(self._genome_tuple)

        if is_genome:
            if not self.assembly_obj_info or not self.genome_obj_info:
                raise ValueError(
                    "Both assembly_obj_info and genome_obj_info must be provided for genome object"
                )

        return is_genome

    def __init__(self,
                 genome_upa: str = None,
                 assembly_upa: str = None,
                 genome_obj_info: list[Any] = None,
                 assembly_obj_info: list[Any] = None,
                 genome_tuple: WSObjTuple = None,
                 assembly_tuple: WSObjTuple = None):

        self._genome_upa = genome_upa
        self._assembly_upa = assembly_upa
        self._genome_obj_info = genome_obj_info
        self._assembly_obj_info = assembly_obj_info
        self._genome_tuple = genome_tuple
        self._assembly_tuple = assembly_tuple

        self._validate_upa()

        # TODO: In case of missing assembly_tuple, we need to build it.
        # This situation might occur during recovery process.
        if not self._assembly_tuple:
            # check the existence of fasta file in the source directory
            # if missing, download fasta file from WS and save it to the source directory
            raise ValueError("assembly_tuple is always required")

        self.is_genome = self._check_is_genome()

    @property
    def genome_upa(self):
        """
        Returns the UPA of the genome object (in format of wsid_objid_ver)
        """
        return self._genome_upa

    @property
    def assembly_upa(self):
        """
        Returns the UPA of the assembly object (in format of wsid_objid_ver)
        """
        return self._assembly_upa

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
