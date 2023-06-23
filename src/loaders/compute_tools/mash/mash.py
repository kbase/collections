"""
Run Mash on a set of assemblies.
"""
import json
import os
from pathlib import Path

from src.loaders.common.loader_common_names import MASH_METADATA
from src.loaders.compute_tools.tool_common import ToolRunner, run_command, unpack_gz_file

KMER_SIZE = 19
SKETCH_SIZE = 10000


def _run_mash_single(
        kbase_id: str,
        source_file: Path,
        output_dir: Path,
        debug: bool,
        kmer_size: int = KMER_SIZE,
        sketch_size: int = SKETCH_SIZE) -> None:
    # RUN mash sketch for a single genome
    command = ['mash', 'sketch',
               '-o', source_file,  # Output prefix.
                                   # Save result file to source file directory. The suffix '.msh' will be appended.
               '-k', f'{kmer_size}',
               '-s', f'{sketch_size}',
               source_file]

    run_command(command, output_dir if debug else None)

    # Save run info to a metadata file in the output directory for parsing later
    metadata_file = output_dir / MASH_METADATA
    metadata = {'source_file': str(source_file),
                # Append '.msh' to the source file name to generate the sketch file name (default by Mash sketch)
                'sketch_file': str(source_file) + '.msh',
                'kmer_size': kmer_size,
                'sketch_size': sketch_size}
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=4)


def main():
    runner = ToolRunner("mash")
    runner.parallel_single_execution(_run_mash_single, unzip=True)


if __name__ == "__main__":
    main()
