"""
Run checkm2 on a set of assemblies.
"""
import time
from pathlib import Path
from typing import Dict

from src.common.storage.field_names import FLD_KBASE_ID
from src.loaders.compute_tools.tool_common import (
    GenomeTuple,
    ToolRunner,
    run_command,
    write_fatal_tuples_to_dict,
    create_fatal_tuple,
)
from src.loaders.compute_tools.tool_result_parser import (
    process_genome_attri_result,
)

# The following features will be extracted from the CheckM2 result quality_report.tsv file as computed genome attributes
# If empty, select all available fields
SELECTED_CHECKM2_FEATURES = {'Completeness', 'Contamination'}


def _run_checkm2(
        ids_to_files: Dict[str, GenomeTuple],
        output_dir: Path,
        threads: int,
        debug: bool,
):
    size = len(ids_to_files)
    print(f'Start executing checkM2 for {size} genomes')
    start = time.time()

    # RUN checkM2 predict
    command = ['checkm2', 'predict',
               '--output-directory', str(output_dir),
               '--threads', str(threads),
               '--force',  # will overwrite output directory contents
               '--input'] + [str(v.source_file) for v in ids_to_files.values()]

    command.append('--debug') if debug else None
    print(f'running {" ".join(command)}')
    # checkm2 will clear output_dir before it starts, which will delete any log files
    log_dir = output_dir.parent / ("checkm2_log_" + output_dir.parts[-1])
    run_command(command, log_dir if debug else None)
    end_time = time.time()
    print(f"Used {round((end_time - start) / 60, 2)} minutes to execute checkM2 predict "
          + f"for {size} genomes"
          )

    tool_file_name, genome_id_col = 'quality_report.tsv', 'Name'
    genome_attri_docs = process_genome_attri_result(output_dir,
                                                    SELECTED_CHECKM2_FEATURES,
                                                    genome_id_col,
                                                    ids_to_files,
                                                    [tool_file_name],
                                                    )

    # create fatal error file if any genome is missing from the result file
    fatal_tuples = []
    error_message = f"Result missing in the CheckM2 output file."
    success_ids = [genome_attri_doc[FLD_KBASE_ID] for genome_attri_doc in genome_attri_docs]
    missing_ids = set(ids_to_files.keys()) - set(success_ids)
    print(f"Found {len(missing_ids)} genomes missing from the CheckM2 output file.")
    for missing_id in missing_ids:
        fatal_tuple = create_fatal_tuple(missing_id, ids_to_files, error_message)
        fatal_tuples.append(fatal_tuple)
    write_fatal_tuples_to_dict(fatal_tuples, output_dir)


def main():
    runner = ToolRunner("checkm2", tool_data_id_from_filename=True)
    runner.parallel_batch_execution(_run_checkm2)


if __name__ == "__main__":
    main()
