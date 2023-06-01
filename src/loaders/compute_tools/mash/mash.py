"""
Run Mash on a set of assemblies.
"""
import json
import threading
import time
from pathlib import Path
from typing import Dict

from src.loaders.compute_tools.tool_common import ToolRunner, run_command


def _run_mash_single(
        genome_id: str,
        source_file: Path,
        output_dir: Path,
        debug: bool,
        kmer_size: int = 19,
        sketch_size: int = 10000) -> None:
    # RUN mash sketch for a single genome
    command = ['mash', 'sketch',
               '-o', source_file,  # Output prefix.
                                   # Save result file to source file directory. The suffix '.msh' will be appended.
               '-k', f'{kmer_size}',
               '-s', f'{sketch_size}',
               source_file]

    run_command(command, output_dir / f'{genome_id}' if debug else None)

    # Save run info to a metadata file in the output directory for parsing later
    metadata_file = output_dir / f'{genome_id}' / f'mash_run_metadata.json'
    metadata = {'source_file': str(source_file), 'kmer_size': kmer_size, 'sketch_size': sketch_size}
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=4)


def _run_mash(
        ids_to_files: Dict[str, Path],
        output_dir: Path,
        threads_num: int,
        debug: bool) -> None:
    size = len(ids_to_files)
    print(f'Start executing Mash for {size} genomes')
    start = time.time()
    semaphore = threading.BoundedSemaphore(max(1, threads_num))
    threads = []

    for genome_id, source_file in ids_to_files.items():
        semaphore.acquire()  # Acquire a semaphore to limit the number of concurrent threads
        thread = threading.Thread(target=_run_mash_single, args=(genome_id, source_file, output_dir, debug))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute mash for '
        f'{len(ids_to_files)} genomes')


def main():
    runner = ToolRunner("mash")
    runner.run_batched(_run_mash)


if __name__ == "__main__":
    main()
