"""
Run Mash on a set of assemblies.
"""
import time
from pathlib import Path

from src.loaders.common.loader_common_names import TOOL_METADATA
from src.loaders.compute_tools.tool_common import ToolRunner, run_command, create_tool_metadata

KMER_SIZE = 19
SKETCH_SIZE = 10000


def _run_mash_single(
        tool_safe_data_id: str,
        data_id: str,
        source_file: Path,
        output_dir: Path,
        threads_per_tool_run: int,
        debug: bool,
        kmer_size: int = KMER_SIZE,
        sketch_size: int = SKETCH_SIZE) -> None:
    start = time.time()
    print(f'Start executing Mash for {data_id}')

    metadata_file = output_dir / TOOL_METADATA
    if metadata_file.exists():
        print(f"Skipping {source_file} as it has already been processed.")
        return

    # RUN mash sketch for a single genome
    command = ['mash', 'sketch',
               '-o', source_file,  # Output prefix.
               # Save result file to source file directory. The suffix '.msh' will be appended.
               '-k', f'{kmer_size}',
               '-s', f'{sketch_size}',
               '-p', f'{threads_per_tool_run}',
               source_file]

    run_command(command, output_dir if debug else None)

    end_time = time.time()
    run_time = end_time - start
    print(
        f'Used {round(run_time / 60, 2)} minutes to execute Mash for {data_id}')

    # Save run info to a metadata file in the output directory for parsing later
    additional_metadata = {
        'source_file': str(source_file),
        # Append '.msh' to the source file name to generate the sketch file name (default by Mash sketch)
        'sketch_file': str(source_file) + '.msh',
        'kmer_size': kmer_size,
        'sketch_size': sketch_size,
        'data_id': data_id,
    }
    create_tool_metadata(
        output_dir,
        tool_name="mash",
        version="2.0",
        command=command,
        run_time=round(run_time, 2),
        batch_size=1,
        additional_metadata=additional_metadata)


def main():
    runner = ToolRunner("mash")
    runner.parallel_single_execution(_run_mash_single, unzip=True)


if __name__ == "__main__":
    main()
