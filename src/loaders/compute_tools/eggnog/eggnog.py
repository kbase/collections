"""
Run eggNOG tool on a set of faa files.
"""
import json
from pathlib import Path

from src.loaders.common.loader_common_names import EGGNOG_METADATA
from src.loaders.compute_tools.tool_common import ToolRunner, run_command

INPUT_TYPE = 'proteins'
THREADS = 16


def _run_eggnog_single(
        tool_safe_data_id: str,
        data_id: str,
        source_file: Path,
        output_dir: Path,
        debug: bool) -> None:

    metadata_file = output_dir / EGGNOG_METADATA
    if metadata_file.exists():
        print(f"Skipping {source_file} as it has already been processed.")
        return

    # RUN eggNOG for a single genome
    command = ['emapper.py',
               '-i', source_file,  # Input file.
               '-o', source_file,  # Output prefix.
                                   # Save result file to source file directory. Expecting 'emapper.annotations', 'emapper.hits' and  'emapper.seed_orthologs' files.
               '--itype', f'{INPUT_TYPE}',
               '--cpu', f'{THREADS}',
               '--override'  # Overwrites output files if they exist from previous runs.
               ]

    run_command(command, output_dir if debug else None)

    # Save run info to a metadata file in the output directory for parsing later
    metadata = {'source_file': str(source_file),
                'input_type': INPUT_TYPE}
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=4)


def main():
    runner = ToolRunner("eggNOG")
    runner.parallel_single_execution(_run_eggnog_single, unzip=True)


if __name__ == "__main__":
    main()
