"""
Run the gtdb_tk tool on a set of assemblies.
"""

import time
from src.loaders.compute_tools.tool_common import ToolRunner, run_command


def _run_gtdb_tk(ids_to_files: dict[Path, str], output_dir: Path, threads: int, debug: bool):
    size = len(ids_to_files)
    print(f'Start executing GTDB-TK for {size} genomes')
    start = time.time()

    # create the batch file
    # tab separated in 2 columns (FASTA file, genome ID)
    batch_file_path = batch_dir / f'genome.fasta.list'
    with open(batch_file_path, "w") as batch_file:
        for genome_id, source_file in ids_to_files.items():
            batch_file.write(f'{source_file}\t{genome_id}\n')
    command = ['gtdbtk', 'classify_wf',
               '--batchfile', batch_file_path,
               '--out_dir', output_dir,
               '--force',
               '--cpus', str(threads)
    ]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    run_command(command, debug=debug, log_dir=work_dir / "classify_wf_log")

    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute gtdbtk classify_wf for '
        f'{len(ids_to_files)} genomes')


def main():
    runner = ToolRunner("gtdb_tk")
    runner.run_batched(_run_gtdb_tk)


if __name__ == "__main__":
    main()
