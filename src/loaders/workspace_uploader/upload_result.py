from collections import namedtuple
from typing import Any


# TODO: this struct assumes obj_name is the file name,
#  but not true for downloaded fasta file associated with Assembly object
#  This is okay for now as we are currently focusing solely on Genome/Assembly uploads and not a combination of both.
# i.g. assembly object name: 'GCF_000979555.1_gtlEnvA5udCFS_genomic.gbff.gz_assembly'
#      downloaded fasta file name: 'GCF_000979555.1_gtlEnvA5udCFS_genomic.gbff.gz_assembly.fasta'
WSObjTuple = namedtuple(
    "WSObjTuple",
    ["obj_name", "host_file_dir", "container_internal_file_dir"],
)


class UploadResult:

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
                 assembly_tuple: WSObjTuple = None,
                 assembly_path: str = None):

        self._genome_upa = genome_upa
        self._assembly_upa = assembly_upa
        self._genome_obj_info = genome_obj_info
        self._assembly_obj_info = assembly_obj_info
        self._genome_tuple = genome_tuple
        self._assembly_tuple = assembly_tuple
        self._assembly_path = assembly_path

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
        return self._genome_upa

    @property
    def assembly_upa(self):
        return self._assembly_upa

    @property
    def genome_obj_info(self):
        return self._genome_obj_info

    @property
    def assembly_obj_info(self):
        return self._assembly_obj_info

    @property
    def genome_tuple(self):
        return self._genome_tuple

    @property
    def assembly_tuple(self):
        return self._assembly_tuple

    @property
    def assembly_path(self):
        return self._assembly_path
