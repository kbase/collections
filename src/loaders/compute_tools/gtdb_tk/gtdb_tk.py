"""
Run the gtdb_tk tool on a set of assemblies.
"""

import os
import re
import time
from pathlib import Path
from typing import Dict

import pandas as pd

from src.loaders.common import loader_common_names
from src.loaders.compute_tools.tool_common import (
    FatalTuple, 
    ToolRunner,
    find_gtdbtk_summary_files,
    run_command,
    write_fatal_tuples_to_dict,
)


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
    

    summary_files = find_gtdbtk_summary_files(output_dir)
    if not summary_files:
        raise ValueError(f"No summary files exist for gtdb-tk in the specified "
                         f"batch output directory {output_dir}.")

    selected_cols = [loader_common_names.GTDB_GENOME_ID_COL,
                     loader_common_names.GTDB_CLASSIFICATION_COL]

    metadata_file = os.path.join(output_dir, loader_common_names.GENOME_METADATA_FILE)
    try:
        meta_df = pd.read_csv(metadata_file, sep='\t')
    except Exception as e:
        raise ValueError('Unable to retrieve the genome metadata file') from e
    meta_dict = dict(zip(meta_df[loader_common_names.META_TOOL_IDENTIFIER], 
                         meta_df[loader_common_names.META_DATA_ID]))
    
    fatal_tuples = []
    for summary_file in summary_files:
        summary_file_path = os.path.join(output_dir, summary_file)
        try:
            summary_df = pd.read_csv(summary_file_path, sep='\t', usecols=selected_cols)
        except Exception as e:
            raise ValueError(f"{summary_file} exists, but unable to retrive") from e

        for genome_id, classify_res in zip(summary_df[loader_common_names.GTDB_GENOME_ID_COL],
                                           summary_df[loader_common_names.GTDB_CLASSIFICATION_COL]):
            if classify_res.startswith(loader_common_names.GTDB_UNCLASSIFIED):
                kbase_id = meta_dict[genome_id]
                source_file_path = ids_to_files[genome_id]
                error_message = f"GTDB_tk classification failed: {classify_res}"
                fatal_tuples.append(FatalTuple(kbase_id, error_message, str(source_file_path), None))
    
    write_fatal_tuples_to_dict(fatal_tuples, output_dir)
  

def main():
    runner = ToolRunner("gtdb_tk", suffix_ids=True)
    runner.parallel_batch_execution(_run_gtdb_tk)


if __name__ == "__main__":
    main()
