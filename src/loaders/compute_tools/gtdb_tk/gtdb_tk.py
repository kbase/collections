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
    GenomeTuple,
    ToolRunner,
    create_fatal_tuple,
    find_gtdbtk_summary_files,
    run_command,
    write_fatal_tuples_to_dict,
)


def _get_id_and_error_message_mapping_from_tsv_files(output_dir: Path):
    genome_dict = dict()

    # process filtered.tsv files
    align_dir = output_dir / "align"
    filter_files = [file_name for file_name in os.listdir(align_dir) if 
                    re.search(loader_common_names.GTDB_FILTER_FILE_PATTERN, file_name)]
    
    if not filter_files or len(filter_files) > 2:
        raise ValueError(f"Either or both gtdbtk.ar53.filtered.tsv and "
                         f"gtdbtk.bac120.filtered.tsv files must exist.")
    
    for filter_file in filter_files:
        filter_file_path = os.path.join(align_dir, filter_file)
        genome_dict.update(_get_id_and_error_message_mapping(filter_file_path))
    
    # process failed.tsv file
    identify_dir = output_dir / "identify"
    fail_file_path = os.path.join(identify_dir, loader_common_names.GTDB_FAIL_GENOME_FILE)
    genome_dict.update(_get_id_and_error_message_mapping(fail_file_path))

    return genome_dict


def _get_id_and_error_message_mapping(file_path: str):
    data = pd.read_csv(file_path, sep="\t", header=None)
    res = {genome_id:error_message for genome_id, error_message in zip(data[0], data[1])}
    return res


def _run_gtdb_tk(
        ids_to_files: Dict[str, GenomeTuple],
        output_dir: Path,
        threads: int,
        debug: bool,
):
    size = len(ids_to_files)
    print(f'Start executing GTDB-TK for {size} genomes')
    start = time.time()

    # create the batch file
    # tab separated in 2 columns (FASTA file, genome ID)
    batch_file_path = output_dir / f'genome.fasta.list'
    with open(batch_file_path, "w") as batch_file:
        for tool_safe_data_id, genome_tuple in ids_to_files.items():
            batch_file.write(f'{genome_tuple.source_file}\t{tool_safe_data_id}\n')
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
    
    fatal_tuples = []
    tool_safe_data_ids = set()
    for summary_file in summary_files:
        summary_file_path = os.path.join(output_dir, summary_file)
        try:
            summary_df = pd.read_csv(summary_file_path, sep='\t', usecols=selected_cols)
        except Exception as e:
            raise ValueError(f"{summary_file} exists, but unable to retrive") from e

        for tool_safe_data_id, classify_res in zip(summary_df[loader_common_names.GTDB_GENOME_ID_COL],
                                                   summary_df[loader_common_names.GTDB_CLASSIFICATION_COL]):
            tool_safe_data_ids.add(tool_safe_data_id)
            if classify_res.startswith(loader_common_names.GTDB_UNCLASSIFIED):
                error_message = f"GTDB_tk classification failed: {classify_res}"
                fatal_tuple = create_fatal_tuple(tool_safe_data_id, ids_to_files, error_message)
                fatal_tuples.append(fatal_tuple)
    
    miss_tool_safe_data_ids = set(ids_to_files.keys()) - tool_safe_data_ids
    filtered_or_failed_genome_id_mapping = _get_id_and_error_message_mapping_from_tsv_files(output_dir)
    error_tool_safe_data_ids = miss_tool_safe_data_ids - set(filtered_or_failed_genome_id_mapping.keys())

    if error_tool_safe_data_ids:
        raise ValueError(
            f"Miss {error_tool_safe_data_ids} that are not found in either "
            f"{loader_common_names.GTDB_FILTER_FILE_PATTERN} or "
            f"{loader_common_names.GTDB_FAIL_GENOME_FILE}"
        )
    
    for error_tool_safe_data_id in error_tool_safe_data_ids:
        error_message = filtered_or_failed_genome_id_mapping[error_tool_safe_data_id]
        fatal_tuple = create_fatal_tuple(error_tool_safe_data_id, ids_to_files, error_message)
        fatal_tuples.append(fatal_tuple)

    write_fatal_tuples_to_dict(fatal_tuples, output_dir)


def main():
    runner = ToolRunner("gtdb_tk", suffix_ids=True)
    runner.parallel_batch_execution(_run_gtdb_tk)


if __name__ == "__main__":
    main()
