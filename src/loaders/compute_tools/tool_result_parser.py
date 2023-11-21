import copy
import os
from pathlib import Path
from typing import Dict, List, Set

import jsonlines
import pandas as pd

from src.common.storage.field_names import FLD_KBASE_ID
from src.loaders.compute_tools.tool_common import GenomeTuple

TOOL_GENOME_ATTRI_FILE = "genome_attribs.jsonl"
MICROTRAIT_CELLS = "microtrait_cells.jsonl"
MICROTRAIT_META = "microtrait_meta.jsonl"
MICROTRAIT_DATA = "microtrait_data.jsonl"


def process_genome_attri_result(
        output_dir: Path,
        features: Set[str],
        genome_id_col: str,
        ids_to_files: Dict[str, GenomeTuple],
        result_files: List[str],
        check_file_exists: bool = True,
        prefix: str = ''
) -> List[Dict]:
    """
    process the output files generated by the tool (checkm2, gtdb-tk, etc) to create a format suitable for
    importing into ArangoDB

    :param output_dir: the directory where the tool result files are stored
    :param features: a list of features to be retrieved from the tool result file
    :param genome_id_col: the column name of the genome id in the tool result file
    :param ids_to_files: a dictionary of tool genome ids to the corresponding GenomeTuple
    :param result_files: the list of tool result files
    :param check_file_exists: if True, check if the tool result file exists
    :param prefix: the prefix of the tool result file
    """

    genome_attri_docs = list()
    for tool_file_name in result_files:
        tool_genome_map = {tool_id: genome_tuple.data_id for tool_id, genome_tuple in ids_to_files.items()}

        tool_file = os.path.join(output_dir, tool_file_name)

        docs = list()
        if os.path.exists(tool_file):
            df = _read_tsv_as_df(tool_file, features, genome_id_col=genome_id_col)
            docs = df.apply(_row_to_doc, args=(features, tool_genome_map,
                                               genome_id_col, prefix), axis=1).to_list()
            docs = [doc for doc in docs if doc]
        elif check_file_exists:
            raise FileNotFoundError(f'Tool result file not found: {tool_file}')

        genome_attri_docs.extend(docs)

    output = output_dir / TOOL_GENOME_ATTRI_FILE
    create_jsonl_files(output, genome_attri_docs)

    return genome_attri_docs


def create_jsonl_files(
        file_path: Path,
        docs: list) -> None:
    """
    create jsonl file from the list of documents

    :param file_path: the path to the jsonl file
    :param docs: the list of documents
    """
    print(f'Creating JSONLines import file: {file_path}')
    with jsonlines.open(file_path, mode='w') as writer:
        writer.write_all(docs)


def _row_to_doc(row, features, tool_genome_map, genome_id_col, prefix):
    # Transforms a row from tool result file into ArangoDB collection document

    try:
        genome_id = tool_genome_map[row[genome_id_col]]
    except KeyError as e:
        raise ValueError('Unable to find genome ID') from e

    doc = _create_doc(row, genome_id, features, prefix)

    return doc


def _create_doc(row, genome_id, features, prefix):
    # Select specific columns and prepare them for import into Arango

    # NOTE: The selected column names will have a prefix added to them if pre_fix is not empty.

    # initialize the document with the genome_id
    doc = {
        FLD_KBASE_ID: genome_id,
    }

    # distinguish the selected fields from the original metadata by adding a common prefix to their names
    if features:
        doc.update(row[list(features)].rename(lambda x: prefix + '_' + x if prefix else x).to_dict())
    else:
        doc.update(row.rename(lambda x: prefix + '_' + x if prefix else x).to_dict())

    return doc


def _read_tsv_as_df(file_path, features, genome_id_col=None):
    # Retrieve the desired fields from a TSV file and return the data in a dataframe

    selected_cols = copy.deepcopy(features) if features else None

    if selected_cols and genome_id_col:
        selected_cols.add(genome_id_col)

    df = pd.read_csv(file_path, sep='\t', keep_default_na=False, usecols=selected_cols)

    return df
