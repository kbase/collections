"""
Run checkm2 on a set of assemblies.
"""

from pathlib import Path
import time
from src.loaders.compute_tools.tool_common import ToolRunner, run_command
from typing import Dict


def _run_checkm2(
        ids_to_files: Dict[str, Path],
        output_dir: Path,
        threads: int,
        debug: bool,
        gemome_id_mapping: Dict[str, str],
):
    size = len(ids_to_files)
    print(f'Start executing checkM2 for {size} genomes')
    start = time.time()

    # RUN checkM2 predict
    command = ['checkm2', 'predict',
               '--output-directory', str(output_dir),
               '--threads', str(threads),
               '--force',  # will overwrite output directory contents
               '--input'] + [str(v) for v in ids_to_files.values()]

    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    # checkm2 will clear output_dir before it starts, which will delete any log files
    log_dir = output_dir.parent / ("checkm2_log_" + output_dir.parts[-1])
    run_command(command, log_dir if debug else None)
    end_time = time.time()
    print(f"Used {round((end_time - start) / 60, 2)} minutes to execute checkM2 predict "
        + f"for {size} genomes"
    )


def main():
    runner = ToolRunner("checkm2", tool_data_id_from_filename=True)
    runner.parallel_batch_execution(_run_checkm2)


if __name__ == "__main__":
    main()
