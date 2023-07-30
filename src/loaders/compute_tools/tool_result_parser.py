import copy
import os

import jsonlines
import pandas as pd

from src.loaders.common import loader_common_names

# used as collection_and_field_names.FLD_KBASE_ID
TOOL_KBASE_ID = "kbase_id"
TOOL_GENOME_ATTRI_FILE = "genome_attribs.jsonl"


def _create_metadata_file(
        meta,
        batch_dir
):
    # create tab separated metadata file with tool generated genome identifier,
    # original genome id and source genome file info.

    # create tool genome identifier metadata file
    genome_meta_file_path = os.path.join(batch_dir, loader_common_names.GENOME_METADATA_FILE)
    with open(genome_meta_file_path, "w") as meta_file:
        meta_file.write(f"{loader_common_names.META_TOOL_IDENTIFIER}\t"
                        + f"{loader_common_names.META_DATA_ID}\t"
                        + f"{loader_common_names.META_SOURCE_DIR}\t"
                        + f"{loader_common_names.META_SOURCE_FILE}\t"
                        + f"{loader_common_names.META_UNCOMPRESSED_FILE}\t"
                        + f"{loader_common_names.META_FILE_NAME}\n")
        for genome_id, genome_meta_info in meta.items():
            meta_file.write(
                f'{genome_meta_info[loader_common_names.META_TOOL_IDENTIFIER]}\t'
                + f'{genome_id}\t'
                + f'{genome_meta_info[loader_common_names.META_SOURCE_DIR]}\t'
                + f'{genome_meta_info[loader_common_names.META_SOURCE_FILE]}\t'
                + f'{genome_meta_info.get(loader_common_names.META_UNCOMPRESSED_FILE, "")}\t'
                + f'{genome_meta_info[loader_common_names.META_FILE_NAME]}\n'
            )


def _read_genome_attri_result(batch_result_dir, tool_file_name, features, genome_id_col,
                              prefix=''):
    # process the output file generated by the tool (checkm2, gtdb-tk, etc) to create a format suitable for importing
    # into ArangoDB
    # NOTE: If the tool result file does not exist, return an empty dictionary.

    # retrieve and process the genome metadata file
    metadata_file = os.path.join(batch_result_dir, loader_common_names.GENOME_METADATA_FILE)
    try:
        meta_df = pd.read_csv(metadata_file, sep='\t')
    except Exception as e:
        raise ValueError('Unable to retrieve the genome metadata file') from e
    tool_genome_map = dict(
        zip(meta_df[loader_common_names.META_TOOL_IDENTIFIER], meta_df[loader_common_names.META_DATA_ID]))

    tool_file = os.path.join(batch_result_dir, tool_file_name)
    docs = list()
    if os.path.exists(tool_file):
        df = _read_tsv_as_df(tool_file, features, genome_id_col=genome_id_col)
        docs = df.apply(_row_to_doc, args=(features, tool_genome_map,
                                           genome_id_col, prefix), axis=1).to_list()
        docs = [doc for doc in docs if doc]

    return docs


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
        TOOL_KBASE_ID: genome_id,
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


def _create_meta_lookup(batch_result_dir):
    # Create a hashmap with genome id as the key and metafile name as the value

    meta_lookup = {}

    metadata_file = os.path.join(batch_result_dir, loader_common_names.GENOME_METADATA_FILE)
    try:
        meta_df = pd.read_csv(metadata_file, sep='\t')
    except Exception as e:
        raise ValueError('Unable to retrieve the genome metadata file') from e
    meta_dict = dict(zip(meta_df[loader_common_names.META_DATA_ID], meta_df[loader_common_names.META_FILE_NAME]))
    meta_lookup.update(meta_dict)

    return meta_lookup


def _create_jsonl_files(file_path, docs):
    print(f'Creating JSONLines import file: {file_path}')
    with jsonlines.open(file_path, mode='w') as writer:
        writer.write_all(docs)
