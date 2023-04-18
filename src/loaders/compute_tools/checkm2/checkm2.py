"""
Run checkm2 on a set of assemblies.
"""

import time
from src.loaders.compute_tools.tool_common import ToolRunner, run_command


def _run_checkm2(ids_to_files: dict[Path, str], output_dir: Path, threads: int, debug: bool):
    size = len(ids_to_files)
    print(f'Start executing checkM2 for {size} genomes')
    start = time.time()

    # RUN checkM2 predict
    command = ['checkm2', 'predict',
               '--output-directory', output_dir,
               '--threads', str(threads),
               '--force',  # will overwrite output directory contents
               '--input'] + list(ids_to_files.values())

    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    run_command(command, debug=debug, log_dir=batch_dir / 'checkm2_log')
    end_time = time.time()
    print(f"Used {round((end_time - start) / 60, 2)} minutes to execute checkM2 predict "
        + f"for {size} genomes"
    )


def main():
    runner = ToolRunner("checkm2", tool_data_id_from_filename=True)
    runner.run_batched(_run_checkm2)


if __name__ == "__main__":
    main()
