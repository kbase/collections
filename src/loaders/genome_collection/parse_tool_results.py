"""
PROTOTYPE

This script involves processing tool result files and organizing them into a structured format suitable
for importing into ArangoDB. The resulting JSON file will be used to update (overwrite/insert) the database with the
parsed data.

Note: If the ArangoDB collection has been previously created using a JSON file generated by the tool result
      loader script and if you want to replace the data created by that loader in ArangoDB, it is crucial to ensure
      that the arguments "--load_ver" and "--kbase_collection" are consistent with the ones used in the tool result
      loader script in order to ensure that the same key is generated for the corresponding Arango document.

usage: parse_tool_results.py [-h] --kbase_collection KBASE_COLLECTION --source_ver SOURCE_VER
                             [--env {CI,NEXT,APPDEV,PROD,NONE}] [--load_ver LOAD_VER]
                             [--tools TOOLS [TOOLS ...]] [--root_dir ROOT_DIR] [--check_genome]
                             [--skip_retrieve_sample]

options:
  -h, --help            show this help message and exit

required named arguments:
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name.
  --source_ver SOURCE_VER
                        Version of the source data, which should match the source directory in the
                        collectionssource. (e.g. 207, 214 for GTDB, 2023.06 for GROW/PMI)

optional arguments:
  --env {CI,NEXT,APPDEV,PROD,NONE}
                        Environment containing the data to be processed. (default: PROD)
  --load_ver LOAD_VER   KBase load version (e.g. r207.kbase.1). (defaults to the source version)
  --tools TOOLS [TOOLS ...]
                        Extract results from tools. (default: retrieve all available sub-
                        directories in the [load_ver] directory)
  --root_dir ROOT_DIR   Root directory for the collections project. (default:
                        /global/cfs/cdirs/kbase/collections)
  --check_genome        Ensure a corresponding genome exists for every assembly
  --skip_retrieve_sample
                        Skip parsing associated sample data for each genome object
"""
import argparse
import json
import os
import shutil
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import jsonlines
import pandas as pd

import src.common.storage.collection_and_field_names as names
from src.common.product_models.heatmap_common_models import (
    FIELD_HEATMAP_CELL_ID,
    FIELD_HEATMAP_ROW_CELLS,
    FIELD_HEATMAP_COL_ID,
    FIELD_HEATMAP_CATEGORY,
    FIELD_HEATMAP_COLUMNS,
    FIELD_HEATMAP_CATEGORIES,
    FIELD_HEATMAP_MIN_VALUE,
    FIELD_HEATMAP_MAX_VALUE,
    FIELD_HEATMAP_CELL_VALUE,
    FIELD_HEATMAP_COUNT,
)
from src.common.storage.db_doc_conversions import (
    collection_load_version_key,
    collection_data_id_key,
    data_product_export_types_to_doc,
)
from src.common.storage.field_names import FLD_KBASE_ID
from src.loaders.common import loader_common_names
from src.loaders.common.loader_helper import (
    create_import_files,
    create_global_fatal_dict_doc,
    init_row_doc,
    is_upa_info_complete,
    make_collection_source_dir,
    merge_docs,
    create_import_dir,
    process_columnar_meta,
)
from src.loaders.compute_tools.tool_common import run_command
from src.loaders.compute_tools.tool_result_parser import (
    TOOL_GENOME_ATTRI_FILE,
    MICROTRAIT_CELLS,
    MICROTRAIT_DATA,
    MICROTRAIT_META,
)

# Default result file name root for parsed heatmap data for arango import.
# Collection, load version, tools name will be prepended to this root.
# Categories (meta, rows, cells etc.) will be appended to this root.
HEATMAP_FILE_ROOT = "heatmap_data"

# The following features will be extracted from the CheckM2 result quality_report.tsv file as computed genome attributes
# If empty, select all available fields
SELECTED_CHECKM2_FEATURES = {'Completeness', 'Contamination'}

# The following features will be extracted from the GTDB-TK summary file
# ('gtdbtk.ar53.summary.tsv' or 'gtdbtk.bac120.summary.tsv') as computed genome attributes
# If empty, select all available fields
SELECTED_GTDBTK_SUMMARY_FEATURES = {}

# tools result will be parsed as computed genome attributes
GENOME_ATTR_TOOLS = ['checkm2', 'gtdb_tk']
# tool result will be parsed as heatmap data
HEATMAP_TOOLS = ['microtrait']
# tools result will be checked for fatal error files
ALL_TOOLS = GENOME_ATTR_TOOLS + HEATMAP_TOOLS + ["mash"]

# The suffix for the sequence metadata file name for Assembly Homology service
# (https://github.com/jgi-kbase/AssemblyHomologyService#sequence-metadata-file)
SEQ_METADATA = 'seq_metadata.jsonl'


def _locate_dir(root_dir, env, kbase_collection, load_ver, check_exists=False, tool=''):
    result_dir = os.path.join(root_dir, loader_common_names.COLLECTION_DATA_DIR, env, kbase_collection, load_ver, tool)

    if check_exists and not (os.path.exists(result_dir) and os.path.isdir(result_dir)):
        raise ValueError(f"Result directory for computed genome attributes of "
                         f"KBase Collection: {kbase_collection}, Env: {env} and Load Version: {load_ver} "
                         f"could not be found.")

    return result_dir


def _update_docs_with_upa_info(res_dict, meta_lookup, check_genome):
    # Update original docs with UPA information through a meta hashmap

    # Keep a set of the encountered types for the kbcoll_export_types
    encountered_types = set()

    for genome_id in res_dict:
        try:
            meta_filename = meta_lookup[genome_id]
        except KeyError as e:
            raise ValueError('Unable to find genome ID') from e

        upa_dict = {}
        if not pd.isna(meta_filename):
            if not is_upa_info_complete(os.path.dirname(meta_filename)):
                raise ValueError(f"{meta_filename} has incomplete upa info. Needs to be redownloaded")
            with open(meta_filename, "r") as json_file:
                upa_info = json.load(json_file)
            object_type = upa_info["type"].split("-")[0]
            upa_dict[object_type] = upa_info["upa"]
            encountered_types.add(object_type)

            # add genome_upa info into _upas dict
            if upa_info.get("genome_upa"):
                upa_dict[loader_common_names.OBJECTS_NAME_GENOME] = upa_info["genome_upa"]
                encountered_types.add(loader_common_names.OBJECTS_NAME_GENOME)
            elif check_genome:
                raise ValueError(f'There is no genome_upa for assembly {upa_info["upa"]}')

        res_dict[genome_id].update({names.FLD_UPA_MAP: upa_dict})

    docs = list(res_dict.values())
    return docs, encountered_types


def _get_batch_dirs(result_dir):
    # Get the list of directories for batches

    batch_dirs = [d for d in os.listdir(result_dir)
                  if os.path.isdir(os.path.join(result_dir, d))
                  and d.startswith(loader_common_names.COMPUTE_OUTPUT_PREFIX)]

    return batch_dirs


def _read_sketch(sketch_file: Path) -> dict:
    # run mash info on the sketch file and return the JSON output

    temp_dir = Path(tempfile.mkdtemp())

    try:
        # Dump sketches in JSON format
        command = ['mash', 'info', sketch_file, '-d']
        print(f'Running mash info: {" ".join(command)}')
        run_command(command, log_dir=temp_dir)

        # Read the mash info output from the stdout file
        with open(temp_dir / 'stdout', 'r') as file:
            json_data = json.load(file)
    finally:
        shutil.rmtree(temp_dir)

    return json_data


def _process_mash_tool(root_dir: str,
                       env: str,
                       kbase_collection: str,
                       load_ver: str,
                       fatal_ids: set[str]):
    # merge and create a single sketch file from result sketch files generated by mash sketch

    result_dir = _locate_dir(root_dir, env, kbase_collection, load_ver, tool='mash')
    batch_dirs = _get_batch_dirs(result_dir)

    sketch_files, seq_meta = list(), list()
    for batch_dir in batch_dirs:
        data_ids = [item for item in os.listdir(os.path.join(result_dir, batch_dir)) if
                    os.path.isdir(os.path.join(result_dir, batch_dir, item))]
        for data_id in data_ids:
            if data_id in fatal_ids:
                continue
            data_dir = Path(result_dir, batch_dir, data_id)
            with open(data_dir / loader_common_names.MASH_METADATA, 'r') as file:
                metadata = json.load(file)

            sketch_file = metadata['sketch_file']
            sketch_data = _read_sketch(sketch_file)
            sketches = sketch_data['sketches']
            if len(sketches) != 1:
                raise ValueError(f'Expected only one sketch in the mash info output for genome: {data_id}')
            sketch_id = sketches[0]['name']
            if sketch_id != metadata['source_file']:
                raise ValueError(f'Expected the sketch name to be the same as the source file name for genome: '
                                 f'{data_id}')
            seq_meta.append({'sourceid': data_id, 'id': sketch_id})
            if not os.path.exists(sketch_file):
                raise ValueError(f'Unable to locate the sketch file: {sketch_file} for genome: {data_id}')
            sketch_files.append(sketch_file)

    create_import_files(root_dir,
                        env,
                        kbase_collection,
                        load_ver,
                        f'{kbase_collection}_{load_ver}_{SEQ_METADATA}',
                        seq_meta)

    import_dir = create_import_dir(root_dir, env, kbase_collection, load_ver)
    mash_output_prefix = import_dir / f'{kbase_collection}_{load_ver}_merged_sketch'

    # write the lines from sketch_files into a temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=True) as temp_file:
        temp_file.write('\n'.join(sketch_files))
        temp_file_path = temp_file.name
        temp_file.flush()

        # run mash paste
        command = ['mash', 'paste', str(mash_output_prefix), '-l', temp_file_path]
        print(f'Running mash paste: {" ".join(command)}')
        run_command(command)


def _process_heatmap_tools(heatmap_tools: set[str],
                           root_dir: str,
                           env: str,
                           kbase_collection: str,
                           load_ver: str,
                           fatal_ids: set[str]):
    # parse result files generated by heatmap tools such as microtrait

    for tool in heatmap_tools:
        try:
            parse_ops = getattr(sys.modules[__name__], tool)
        except AttributeError as e:
            raise ValueError(f'Please implement parsing method for: [{tool}]') from e

        heatmap_meta_dict, heatmap_rows_list, heatmap_cell_details_list = parse_ops(
            root_dir, env, kbase_collection, load_ver, fatal_ids)

        meta_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{names.COLL_MICROTRAIT_META}.jsonl'
        rows_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{names.COLL_MICROTRAIT_DATA}.jsonl'
        cell_details_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{names.COLL_MICROTRAIT_CELLS}.jsonl'

        create_import_files(root_dir, env, kbase_collection, load_ver, meta_output, [heatmap_meta_dict])
        create_import_files(root_dir, env, kbase_collection, load_ver, rows_output, heatmap_rows_list)
        create_import_files(root_dir, env, kbase_collection, load_ver, cell_details_output, heatmap_cell_details_list)


def _process_fatal_error_tools(check_fatal_error_tools: set[str],
                               root_dir: str,
                               env: str,
                               kbase_collection: str,
                               load_ver: str):
    # process fatal error files from a set of check tools and write it to merged file

    if not check_fatal_error_tools:
        return set()

    fatal_dict = dict()
    for tool in check_fatal_error_tools:
        result_dir = _locate_dir(root_dir, env, kbase_collection, load_ver, tool=tool)
        batch_dirs = _get_batch_dirs(result_dir)
        batch_no_batch_prefix = loader_common_names.COMPUTE_OUTPUT_PREFIX + loader_common_names.COMPUTE_OUTPUT_NO_BATCH
        if len(batch_dirs) == 1 and batch_dirs[0].startswith(batch_no_batch_prefix):
            batch_dirs = [os.path.join(batch_dirs[0], d) for d in os.listdir(os.path.join(result_dir, batch_dirs[0]))
                          if os.path.isdir(os.path.join(result_dir, batch_dirs[0], d))]
        for batch_dir in batch_dirs:
            data_dir = os.path.join(result_dir, batch_dir)
            fatal_error_file = os.path.join(data_dir, loader_common_names.FATAL_ERROR_FILE)
            if not os.path.exists(fatal_error_file):
                continue
            try:
                with open(fatal_error_file, "r") as json_file:
                    fatal_errors = json.load(json_file)
            except Exception as e:
                raise ValueError(f"{fatal_error_file} exists, but unable to retrieve") from e

            for kbase_id in fatal_errors:
                fatal_dict_info = create_global_fatal_dict_doc(
                    tool,
                    fatal_errors[kbase_id][loader_common_names.FATAL_ERROR],
                    fatal_errors[kbase_id][loader_common_names.FATAL_STACKTRACE])
                if fatal_dict.get(kbase_id):
                    fatal_dict[kbase_id][loader_common_names.FATAL_ERRORS].append(
                        fatal_dict_info)
                else:
                    fatal_dict[kbase_id] = {
                        loader_common_names.FATAL_FILE: fatal_errors[kbase_id][loader_common_names.FATAL_FILE],
                        loader_common_names.FATAL_ERRORS: [fatal_dict_info]}

    import_dir = create_import_dir(root_dir, env, kbase_collection, load_ver)
    fatal_output = f"{kbase_collection}_{load_ver}_{loader_common_names.FATAL_ERROR_FILE}"
    fatal_error_path = os.path.join(import_dir, fatal_output)
    print(f"Creating a merged {loader_common_names.FATAL_ERROR_FILE}: {fatal_error_path}")
    with open(fatal_error_path, "w") as outfile:
        json.dump(fatal_dict, outfile, indent=4)

    return set(fatal_dict.keys())


def _retrieve_pre_processed_docs(tool: str,
                                 root_dir: str,
                                 env: str,
                                 kbase_collection: str,
                                 load_ver: str,
                                 fatal_ids: set[str]):
    # retrieve pre-processed docs from the tool computation step

    result_dir = _locate_dir(root_dir, env, kbase_collection, load_ver, tool=tool)
    batch_dirs = _get_batch_dirs(result_dir)

    docs = list()
    for batch_dir in batch_dirs:
        attribs_file = os.path.join(result_dir, batch_dir, TOOL_GENOME_ATTRI_FILE)
        with open(attribs_file, 'r') as jsonl_file:
            lines = jsonl_file.readlines()
            for line in lines:
                parsed_line = json.loads(line)
                data_id = parsed_line.get(FLD_KBASE_ID)
                if not data_id:
                    raise ValueError(f'Unable to find {FLD_KBASE_ID} in {parsed_line} from {attribs_file}')

                if data_id not in fatal_ids:
                    init_doc = init_row_doc(kbase_collection, load_ver, data_id)
                    parsed_line.update(init_doc)
                    docs.append(parsed_line)

    return docs


def _process_genome_attri_tools(genome_attr_tools: set[str],
                                root_dir: str,
                                env: str,
                                kbase_collection: str,
                                load_ver: str,
                                check_genome: bool,
                                fatal_ids: set[str],
                                data_id_sample_id_map: dict[str, str]):
    # parse result files generated by genome attribute tools such as checkm2, gtdb-tk, etc

    if not genome_attr_tools:
        return

    genome_attr_tools = sorted(genome_attr_tools)  # sort the tools to ensure consistent order of the output
    docs = list()
    for tool in genome_attr_tools:
        tool_docs = _retrieve_pre_processed_docs(tool, root_dir, env, kbase_collection, load_ver, fatal_ids)
        docs.extend(tool_docs)
    docs = merge_docs(docs, '_key')

    res_dict = {row[names.FLD_KBASE_ID]: row for row in docs}
    meta_lookup = _create_meta_lookup(root_dir, env, kbase_collection, load_ver, tool)
    docs, encountered_types = _update_docs_with_upa_info(res_dict, meta_lookup, check_genome)

    for doc in docs:
        doc[names.FLD_KB_SAMPLE_ID] = data_id_sample_id_map.get(doc[names.FLD_KBASE_ID])

    docs, meta_doc = process_columnar_meta(docs, kbase_collection, load_ver)

    output = f'{kbase_collection}_{load_ver}_{"_".join(genome_attr_tools)}_{names.COLL_GENOME_ATTRIBS}.jsonl'
    create_import_files(root_dir, env, kbase_collection, load_ver, output, docs)

    meta_output = f'{kbase_collection}_{load_ver}_{"_".join(genome_attr_tools)}_{names.COLL_GENOME_ATTRIBS_META}.jsonl'
    create_import_files(root_dir, env, kbase_collection, load_ver, meta_output, [meta_doc])

    export_types_output = f'{kbase_collection}_{load_ver}_{names.COLL_EXPORT_TYPES}.jsonl'
    types_doc = data_product_export_types_to_doc(
        kbase_collection,
        names.GENOME_ATTRIBS_PRODUCT_ID,
        load_ver,
        sorted(encountered_types)
    )
    create_import_files(root_dir, env, kbase_collection, load_ver, export_types_output, [types_doc])


def _create_meta_lookup(root_dir, env, kbase_collection, load_ver, tool):
    # Create a hashmap with genome id as the key and metafile name as the value

    meta_lookup = {}
    result_dir = _locate_dir(root_dir, env, kbase_collection, load_ver, tool=tool)
    batch_dirs = _get_batch_dirs(result_dir)
    for batch_dir in batch_dirs:
        batch_result_dir = os.path.join(result_dir, batch_dir)
        metadata_file = os.path.join(batch_result_dir, loader_common_names.GENOME_METADATA_FILE)
        try:
            meta_df = pd.read_csv(metadata_file, sep='\t')
        except Exception as e:
            raise ValueError('Unable to retrieve the genome metadata file') from e
        meta_dict = dict(zip(meta_df[loader_common_names.META_DATA_ID], meta_df[loader_common_names.META_FILE_NAME]))
        meta_lookup.update(meta_dict)

    return meta_lookup


def _ensure_list_ordered(a_list: list[str]) -> bool:
    # Given a list of int strings, check if the list is ordered in ascending order
    return a_list == sorted(a_list, key=int)


def _build_heatmap_meta(
        reference_meta: list[dict],
        kbase_collection: str,
        load_ver: str,
        min_value: float,
        max_value: float,
        total_rows: int,
) -> dict:
    # Build the heatmap metadata from the reference metadata (list of metadata)

    heatmap_categories = dict()
    for meta in reference_meta:
        category = meta[FIELD_HEATMAP_CATEGORY]
        if category not in heatmap_categories:
            heatmap_categories[category] = {FIELD_HEATMAP_CATEGORY: category, FIELD_HEATMAP_COLUMNS: []}

        meta_without_category = meta.copy()
        del meta_without_category[FIELD_HEATMAP_CATEGORY]
        heatmap_categories[category][FIELD_HEATMAP_COLUMNS].append(meta_without_category)

    # sort the categories by the first column id
    sorted_categories = sorted(
        heatmap_categories.values(),
        key=lambda category: int(category[FIELD_HEATMAP_COLUMNS][0][FIELD_HEATMAP_COL_ID])
    )

    # this is to ensure that the column ids are in ascending order within each category
    # TODO: might need to skip this step for heatmap products other than microtrait
    for category in sorted_categories:
        column_ids = [column[FIELD_HEATMAP_COL_ID] for column in category[FIELD_HEATMAP_COLUMNS]]
        if not _ensure_list_ordered(column_ids):
            raise ValueError(f'Column ids are not ordered in ascending order: {column_ids}')

    heatmap_meta = {FIELD_HEATMAP_CATEGORIES: sorted_categories,
                    FIELD_HEATMAP_MIN_VALUE: min_value,
                    FIELD_HEATMAP_MAX_VALUE: max_value,
                    FIELD_HEATMAP_COUNT: total_rows,
                    names.FLD_ARANGO_KEY: collection_load_version_key(kbase_collection, load_ver),
                    names.FLD_COLLECTION_ID: kbase_collection,
                    names.FLD_LOAD_VERSION: load_ver}

    return heatmap_meta


def microtrait(root_dir, env, kbase_collection, load_ver, fatal_ids):
    """
    Parse and format result files as heatmap data generated by the MicroTrait tool.
    """

    result_dir = _locate_dir(root_dir, env, kbase_collection, load_ver, tool='microtrait')
    batch_dirs = _get_batch_dirs(result_dir)

    # NOTE: If we ever need to modify the logic to accommodate changes in the heatmap data structure,
    # it's probable that we will also need to make corresponding updates to the logic in parse_PMI_biolog_data.py.

    heatmap_cell_details, heatmap_rows, reference_meta = list(), list(), None
    min_value, max_value = float('inf'), float('-inf')
    for batch_dir in batch_dirs:
        data_ids = [item for item in os.listdir(os.path.join(result_dir, batch_dir)) if
                    os.path.isdir(os.path.join(result_dir, batch_dir, item)) and item not in fatal_ids]

        for data_id in data_ids:
            data_dir = Path(result_dir, batch_dir, data_id)

            # process heatmap cell details
            with jsonlines.open(data_dir / MICROTRAIT_CELLS, 'r') as jsonl_f:
                for cell in jsonl_f:
                    cell[names.FLD_COLLECTION_ID] = kbase_collection
                    cell[names.FLD_LOAD_VERSION] = load_ver
                    cell[names.FLD_ARANGO_KEY] = collection_data_id_key(kbase_collection,
                                                                        load_ver,
                                                                        cell[FIELD_HEATMAP_CELL_ID])
                    heatmap_cell_details.append(cell)

            # process heatmap rows
            with jsonlines.open(data_dir / MICROTRAIT_DATA, 'r') as jsonl_f:
                for data in jsonl_f:
                    cells = data[FIELD_HEATMAP_ROW_CELLS]
                    for cell in cells:
                        cell_val = cell[FIELD_HEATMAP_CELL_VALUE]
                        min_value = min(min_value, cell_val)
                        max_value = max(max_value, cell_val)
                    data[FIELD_HEATMAP_ROW_CELLS] = cells
                    heatmap_rows.append(dict(data,
                                             **init_row_doc(kbase_collection, load_ver, data[names.FLD_KBASE_ID])))

            # process heatmap metadata
            with jsonlines.open(data_dir / MICROTRAIT_META, 'r') as jsonl_f:
                # The microtrait runner saves the metadata as jsonl with 1 trait column per line
                metas = [meta for meta in jsonl_f]

                # reading the metadata from the first data dir as the metadata should be consistent across all data dirs
                if reference_meta is None:
                    reference_meta = metas

                # check if the metadata is consistent across all data dirs
                if metas != reference_meta:
                    raise ValueError(f'Inconsistent metadata for {data_dir}')

    heatmap_meta = _build_heatmap_meta(
        reference_meta, kbase_collection, load_ver, min_value, max_value, len(heatmap_rows))

    return heatmap_meta, heatmap_rows, heatmap_cell_details


def _flat_samples_data(prepared_samples_data: list[dict]) -> list[dict]:
    # Flatten the sample data to a list of documents with each document containing a single sample
    #
    # Parameters:
    # The 'prepared_samples_data' variable from the previous step comprises a list of dictionaries formatted
    # for ArangoDB import, each containing sample data for a single genome marked as 'kbase_id'.
    # Consequently, some entries in the list may contain identical sample data.
    #
    # Return:
    # This function flattens the sample data into a list of dictionaries. Each dictionary retains identical sample data,
    # accompanied by a list of all associated genomes marked as 'kbase_ids'.
    # Additionally, the function introduces a 'genome_count' field and regenerates the '_key' field for each dictionary.
    # All other fields, such as '_mtchsel', 'coll', 'load_ver', and the sample data fields, remain unchanged from the input.

    flatten_samples_data = defaultdict(lambda: {names.FLD_KBASE_IDS: list()})

    for sample_data in prepared_samples_data:
        kbase_sample_id = sample_data[names.FLD_KB_SAMPLE_ID]
        kbase_id = sample_data[names.FLD_KBASE_ID]

        if not flatten_samples_data[kbase_sample_id].get(names.FLD_KBASE_IDS):
            flatten_samples_data[kbase_sample_id].update(sample_data)

        flatten_samples_data[kbase_sample_id][names.FLD_KBASE_IDS].append(kbase_id)

    # Generate new _key and add genome count for each unique entry
    for entry in flatten_samples_data.values():
        entry[names.FLD_ARANGO_KEY] = collection_data_id_key(entry[names.FLD_COLLECTION_ID],
                                                             entry[names.FLD_LOAD_VERSION],
                                                             entry[names.FLD_KB_SAMPLE_ID])
        entry[names.FLD_KB_GENOME_COUNT] = len(entry[names.FLD_KBASE_IDS])
        del entry[names.FLD_KBASE_ID]

    return flatten_samples_data.values()


def _retrieve_sample(root_dir, env, kbase_collection, source_ver, load_ver):
    print(f'Parsing sample data for {kbase_collection} collection, load version {load_ver}, '
          f'source version {source_ver}.')
    source_dir = make_collection_source_dir(root_dir, env, kbase_collection, source_ver)
    if not os.path.exists(source_dir):
        raise ValueError(f'The source directory {source_dir} does not exist.')

    data_ids = [data_dir for data_dir in os.listdir(source_dir) if os.path.isdir(os.path.join(source_dir, data_dir))]
    prepared_samples_data = list()
    for data_id in data_ids:
        data_dir = os.path.join(source_dir, data_id)
        prepared_sample_files = [file for file in os.listdir(data_dir) if
                                 file.endswith(loader_common_names.SAMPLE_PREPARED_EXT)]

        if len(prepared_sample_files) != 1:
            raise ValueError(
                f'Expected to find one prepared sample file in {data_dir} but found {prepared_sample_files}.')

        # generated by workspace_downloader.py - _download_sample_data
        prepared_sample_file = os.path.join(data_dir, prepared_sample_files[0])

        with open(prepared_sample_file, 'r') as file:
            sample_data = json.load(file)

        doc = init_row_doc(kbase_collection, load_ver, data_id)
        doc.update(sample_data)

        prepared_samples_data.append(doc)

    data_id_sample_id_map = {doc[names.FLD_KBASE_ID]: doc[names.FLD_KB_SAMPLE_ID] for doc in prepared_samples_data}

    flatten_samples_data = _flat_samples_data(prepared_samples_data)
    create_import_files(root_dir,
                        env,
                        kbase_collection,
                        load_ver,
                        f'{kbase_collection}_{load_ver}_{names.COLL_SAMPLES}.jsonl',
                        flatten_samples_data)

    return data_id_sample_id_map


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Generate a JSON file for importing into ArangoDB by parsing computed '
                    'genome attributes.')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required flag arguments
    required.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)

    required.add_argument(f'--{loader_common_names.SOURCE_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.SOURCE_VER_DESCR)

    # Optional arguments
    optional.add_argument(
        f"--{loader_common_names.ENV_ARG_NAME}",
        type=str,
        choices=loader_common_names.KB_ENV + [loader_common_names.DEFAULT_ENV],
        default='PROD',
        help="Environment containing the data to be processed. (default: PROD)",
    )

    optional.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', type=str,
                          help=loader_common_names.LOAD_VER_DESCR + ' (defaults to the source version)')

    optional.add_argument('--tools', type=str, nargs='+',
                          help=f'Extract results from tools. '
                               f'(default: retrieve all available sub-directories in the '
                               f'[{loader_common_names.LOAD_VER_ARG_NAME}] directory)')
    optional.add_argument(
        f'--{loader_common_names.ROOT_DIR_ARG_NAME}',
        type=str,
        default=loader_common_names.ROOT_DIR,
        help=f'{loader_common_names.ROOT_DIR_DESCR} (default: {loader_common_names.ROOT_DIR})'
    )
    optional.add_argument('--check_genome', action="store_true",
                          help='Ensure a corresponding genome exists for every assembly')
    optional.add_argument(
        "--skip_retrieve_sample",
        action="store_true",
        help="Skip parsing associated sample data for each genome object",
    )
    args = parser.parse_args()

    tools = [tool.lower() for tool in args.tools] if args.tools else None
    env = getattr(args, loader_common_names.ENV_ARG_NAME)
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    source_ver = getattr(args, loader_common_names.SOURCE_VER_ARG_NAME)
    load_ver = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)
    if not load_ver:
        load_ver = source_ver
    check_genome = args.check_genome

    data_id_sample_id_map = dict()
    if not args.skip_retrieve_sample:
        data_id_sample_id_map = _retrieve_sample(root_dir, env, kbase_collection, source_ver, load_ver)

    result_dir = _locate_dir(root_dir, env, kbase_collection, load_ver, check_exists=True)

    executed_tools = [d for d in os.listdir(result_dir) if os.path.isdir(os.path.join(result_dir, d))]
    if not executed_tools:
        raise ValueError(f'Cannot find any tool result folders in {result_dir}')

    tools = executed_tools if not tools else tools
    if set(tools) - set(executed_tools):
        raise ValueError(f'Please ensure that all tools have been successfully executed. '
                         f'Only the following tools have already been run: {executed_tools}')

    fatal_ids = _process_fatal_error_tools(set(ALL_TOOLS).intersection(tools), root_dir, env, kbase_collection,
                                           load_ver)

    _process_genome_attri_tools(set(GENOME_ATTR_TOOLS).intersection(tools),
                                root_dir,
                                env,
                                kbase_collection,
                                load_ver,
                                check_genome,
                                fatal_ids,
                                data_id_sample_id_map,
                                )

    _process_heatmap_tools(set(HEATMAP_TOOLS).intersection(tools), root_dir, env, kbase_collection, load_ver, fatal_ids)

    if 'mash' in tools:
        _process_mash_tool(root_dir, env, kbase_collection, load_ver, fatal_ids)


if __name__ == "__main__":
    main()
