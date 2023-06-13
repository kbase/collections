"""
Run the gtdb_tk tool on a set of assemblies.
"""

import os
import pandas as pd
import time
from pathlib import Path
from typing import Dict

from src.loaders.common import loader_common_names, loader_helper
from src.loaders.compute_tools.tool_common import ToolRunner, run_command


def _run_gtdb_tk(ids_to_files: Dict[Path, str], output_dir: Path, threads: int, debug: bool):
    size = len(ids_to_files)
    print(f'Start executing GTDB-TK for {size} genomes')
    start = time.time()

    # create the batch file
    # tab separated in 2 columns (FASTA file, genome ID)
    batch_file_path = output_dir / f'genome.fasta.list'
    with open(batch_file_path, "w") as batch_file:
        for genome_id, source_file in ids_to_files.items():
            batch_file.write(f'{source_file}\t{genome_id}\n')
    command = ['gtdbtk', 'classify_wf',
               '--batchfile', str(batch_file_path),
               '--out_dir', str(output_dir),
               '--force',
               '--cpus', str(threads)
    ]
    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    run_command(command, output_dir / "classify_wf_log" if debug else None)

    end_time = time.time()
    print(
        f'Used {round((end_time - start) / 60, 2)} minutes to execute gtdbtk classify_wf for '
        f'{len(ids_to_files)} genomes')
    
    fatal_dict = {}
    summary_file_exists = False
    summary_files = [loader_common_names.GTDBTK_AR53_SUMMARY_FILE,
                     loader_common_names.GTDBTK_BAC120_SUMMARY_FILE]
    selected_cols = [loader_common_names.GENOME_ID_COL,
                     loader_common_names.CLASSIFICATION_COL]
    for summary_file in summary_files:
        summary_file_path = os.path.join(output_dir, summary_file)
        if not os.path.exists(summary_file_path):
            continue
        try:
            summary_df = pd.read_csv(summary_file_path, sep='\t', usecols=selected_cols)
        except Exception as e:
            raise ValueError(f"{summary_file} exists, but unable to retrive") from e
        summary_file_exists = True
        for genome_id, classfiy_res in zip(summary_df[loader_common_names.GENOME_ID_COL],
                                           summary_df[loader_common_names.CLASSIFICATION_COL]):
            if classfiy_res.startswith(loader_common_names.UNCLASSIFIED):
                kbase_id = genome_id[:genome_id.index(loader_common_names.GENOME_ID_SUFFIX)]
                source_file_path = ids_to_files[genome_id]
                error_message = f"GTDB_tk classification failed: {classfiy_res}"
                fatal_dict[kbase_id] = loader_helper.create_fatal_dict_doc(
                    error_message, str(source_file_path))

    if not summary_file_exists:
        raise ValueError(f"Unable to process the summary files for gtdb-tk in the specified "
                         f"batch output directory {output_dir}.")

    fatal_error_path = os.path.join(output_dir, loader_common_names.FATAL_ERROR_FILE)
    with open(fatal_error_path, "w") as outfile:
        outfile.dump(fatal_dict, outfile)


def main():
    runner = ToolRunner("gtdb_tk", suffix_ids=True)
    runner.parallel_batch_execution(_run_gtdb_tk)


if __name__ == "__main__":
    main()
