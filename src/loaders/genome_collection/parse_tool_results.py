"""
PROTOTYPE

This script involves processing tool result files and organizing them into a structured format suitable
for importing into ArangoDB. The resulting JSON file will be used to update (overwrite/insert) the database with the
parsed data.

Note: If the ArangoDB collection has been previously created using a JSON file generated by the tool result
      loader script and if you want to replace the data created by that loader in ArangoDB, it is crucial to ensure
      that the arguments "--load_ver" and "--kbase_collection" are consistent with the ones used in the tool result
      loader script in order to ensure that the same key is generated for the corresponding Arango document.

usage: parse_tool_results.py [-h] --load_ver LOAD_VER --kbase_collection KBASE_COLLECTION
                                        [--tools TOOLS [TOOLS ...]] [--root_dir ROOT_DIR] [-o OUTPUT]

options:
  -h, --help            show this help message and exit

required named arguments:
  --load_ver LOAD_VER   KBase load version (e.g. r207.kbase.1).
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name.

optional arguments:
  --tools TOOLS [TOOLS ...]
                        Extract results from tools. (default: retrieve all available sub-directories in the [load_ver]
                        directory)
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)

"""
import argparse
import copy
import json
import os
import sys
from numbers import Number
from typing import Any

import pandas as pd

import src.common.storage.collection_and_field_names as names
from src.common.product_models.heatmap_common_models import (
    HeatMapMeta,
    ColumnInformation,
    ColumnCategory,
    Cell,
    HeatMapRow,
    ColumnType,
    CellDetail,
    CellDetailEntry,
    FIELD_HEATMAP_CELL_ID,
)
from src.common.storage.db_doc_conversions import (
    collection_data_id_key, 
    collection_load_version_key, 
    data_product_export_types_to_doc,
)
from src.loaders.common import loader_common_names
from src.loaders.common.loader_helper import (
    convert_to_json, 
    init_genome_atrri_doc, 
    is_upa_info_complete, 
    merge_docs,
)

# Default result file name suffix for parsed computed genome attributes data for arango import.
# Collection, load version and tools name will be prepended to this file name suffix.
COMPUTED_GENOME_ATTR_FILE_SUFFIX = "computed_genome_attribs.jsonl"

# Default result file name suffix for parsed heatmap data for arango import.
# Collection, load version, tools name and categories (meta, rows, cells etc.) will be prepended to this suffix.
HEATMAP_FILE_SUFFIX = "heatmap_data.jsonl"

# Default result file name suffix for the kbcoll_export_types collections for arango import.
# Collection, load version and types will be prepended to this file name suffix.
KBCALL_EXPORT_TYPES_FILE_SUFFIX = "kbcoll_export_types.jsonl"

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

# The following features will be extracted from the MicroTrait result file as heatmap data
_MICROTRAIT_TRAIT_DISPLAYNAME_SHORT = 'microtrait_trait-displaynameshort'  # used as column name of the trait
_MICROTRAIT_TRAIT_DISPLAYNAME_LONG = 'microtrait_trait-displaynamelong'  # used as description of the trait
_MICROTRAIT_TRAIT_VALUE = 'microtrait_trait-value'  # value of the trait (can be integer or 0/1 as boolean)
_MICROTRAIT_TRAIT_TYPE = 'microtrait_trait-type'  # type of trait (count or binary)
_MICROTRAIT_TRAIT_ORDER = 'microtrait_trait-displayorder'  # order of the trait defined by the granularity table used as the index of trait

# The following features are used to create the heatmap metadata and rows
_SYS_TRAIT_INDEX = 'trait_index'  # index of the trait
_SYS_TRAIT_NAME = 'trait_name'  # name of the trait
_SYS_TRAIT_DESCRIPTION = 'trait_description'  # description of the trait
_SYS_TRAIT_CATEGORY = 'trait_category'  # category of the trait
_SYS_TRAIT_VALUE = 'trait_value'  # value of the trait
_SYS_TRAIT_TYPE = 'trait_type'  # value of the trait

_SYS_DEFAULT_TRAIT_VALUE = 0  # default value (0 or False) for a trait if the value is missing/not available

# The map between the MicroTrait trait names and the corresponding system trait names
# Use the microtrait_trait-name column as the unique identifier for a trait globally,
# the microtrait_trait-displaynameshort column as the column name,
# microtrait_trait-displaynamelong column as the column description, and
# microtrait_trait-value as the cell value
_MICROTRAIT_TO_SYS_TRAIT_MAP = {
    loader_common_names.MICROTRAIT_TRAIT_NAME: loader_common_names.SYS_TRAIT_ID,
    _MICROTRAIT_TRAIT_DISPLAYNAME_SHORT: _SYS_TRAIT_NAME,
    _MICROTRAIT_TRAIT_DISPLAYNAME_LONG: _SYS_TRAIT_DESCRIPTION,
    _MICROTRAIT_TRAIT_VALUE: _SYS_TRAIT_VALUE,
    _MICROTRAIT_TRAIT_TYPE: _SYS_TRAIT_TYPE,
    _MICROTRAIT_TRAIT_ORDER: _SYS_TRAIT_INDEX,
    loader_common_names.DETECTED_GENE_SCORE_COL: loader_common_names.DETECTED_GENE_SCORE_COL,
}

# Default directory name for the parsed JSONL files for arango import
IMPORT_DIR = 'import_files'


def _locate_dir(root_dir, kbase_collection, load_ver, check_exists=False, tool=''):
    result_dir = os.path.join(root_dir, loader_common_names.COLLECTION_DATA_DIR, kbase_collection, load_ver, tool)

    if check_exists and not (os.path.exists(result_dir) and os.path.isdir(result_dir)):
        raise ValueError(f"Result directory for computed genome attributes of "
                         f"KBase Collection: {kbase_collection} and Load Version: {load_ver} could not be found.")

    return result_dir


def _read_tsv_as_df(file_path, features, genome_id_col=None):
    # Retrieve the desired fields from a TSV file and return the data in a dataframe

    selected_cols = copy.deepcopy(features) if features else None

    if selected_cols and genome_id_col:
        selected_cols.add(genome_id_col)

    df = pd.read_csv(file_path, sep='\t', keep_default_na=False, usecols=selected_cols)

    return df


def _create_doc(row, kbase_collection, load_version, genome_id, features, prefix):
    # Select specific columns and prepare them for import into Arango

    # NOTE: The selected column names will have a prefix added to them if pre_fix is not empty.

    doc = init_genome_atrri_doc(kbase_collection, load_version, genome_id)

    # distinguish the selected fields from the original metadata by adding a common prefix to their names
    if features:
        doc.update(row[list(features)].rename(lambda x: prefix + '_' + x if prefix else x).to_dict())
    else:
        doc.update(row.rename(lambda x: prefix + '_' + x if prefix else x).to_dict())

    return doc


def _row_to_doc(row, kbase_collection, load_version, features, tool_genome_map, genome_id_col, prefix):
    # Transforms a row from tool result file into ArangoDB collection document

    try:
        genome_id = tool_genome_map[row[genome_id_col]]
    except KeyError as e:
        raise ValueError('Unable to find genome ID') from e

    doc = _create_doc(row, kbase_collection, load_version, genome_id, features, prefix)

    return doc


def _update_docs_with_upa_info(res_dict, meta_lookup):
    # Update original docs with UPA informathion through a meta hashmap

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

        res_dict[genome_id].update({names.FLD_UPA_MAP: upa_dict})

    docs = list(res_dict.values())
    return docs, encountered_types


def _read_tool_result(result_dir, batch_dir, kbase_collection, load_ver, tool_file_name, features, genome_id_col,
                      prefix=''):
    # process the output file generated by the tool (checkm2, gtdb-tk, etc) to create a format suitable for importing
    # into ArangoDB
    # NOTE: If the tool result file does not exist, return an empty dictionary.

    batch_result_dir = os.path.join(result_dir, str(batch_dir))

    # retrieve and process the genome metadata file
    metadata_file = os.path.join(batch_result_dir, loader_common_names.GENOME_METADATA_FILE)
    try:
        meta_df = pd.read_csv(metadata_file, sep='\t')
    except Exception as e:
        raise ValueError('Unable to retrieve the genome metadata file') from e
    tool_genome_map = dict(zip(meta_df[loader_common_names.META_TOOL_IDENTIFIER], meta_df[loader_common_names.META_DATA_ID]))

    tool_file = os.path.join(result_dir, str(batch_dir), tool_file_name)
    docs = dict()
    if os.path.exists(tool_file):
        df = _read_tsv_as_df(tool_file, features, genome_id_col=genome_id_col)
        docs = df.apply(_row_to_doc, args=(kbase_collection, load_ver, features, tool_genome_map,
                                           genome_id_col, prefix), axis=1).to_list()

    return docs


def _get_batch_dirs(result_dir):
    # Get the list of directories for batches

    batch_dirs = [d for d in os.listdir(result_dir)
                  if os.path.isdir(os.path.join(result_dir, d))
                  and d.startswith(loader_common_names.COMPUTE_OUTPUT_PREFIX)]

    return batch_dirs


def _process_trait(row: dict[str, str | float | int | bool],
                   traits_meta: dict[str, dict[str, str]],
                   traits_val: dict[str, list[dict[str, int | float | bool]]],
                   data_id: str, ):
    # Process a row from the trait file and update the global traits metadata and value lists accordingly

    _append_or_check_trait(traits_meta,
                           row[loader_common_names.SYS_TRAIT_ID],
                           row[_SYS_TRAIT_INDEX],
                           row[_SYS_TRAIT_NAME],
                           row[_SYS_TRAIT_DESCRIPTION],
                           row[_SYS_TRAIT_CATEGORY],
                           row[_SYS_TRAIT_TYPE])
    _append_trait_val(traits_meta,
                      traits_val,
                      row[loader_common_names.SYS_TRAIT_ID],
                      row[_SYS_TRAIT_VALUE],
                      row[loader_common_names.DETECTED_GENE_SCORE_COL],
                      data_id)


def _append_trait_val(
        traits_meta: dict[str, dict[str, str]],
        traits_val: dict[str, list[dict[str, float | int | bool]]],
        trait_id: str,
        trait_value: float | int | bool,
        detected_gene_score: str,
        data_id: str):
    # Append a trait value to the global traits value list

    try:
        trait_index = traits_meta[trait_id][_SYS_TRAIT_INDEX]
    except KeyError as e:
        raise ValueError(f'Unable to find trait ID {trait_id}') from e

    if data_id not in traits_val:
        traits_val[data_id] = list()

    detected_gene_score = json.loads(detected_gene_score)
    traits_val[data_id].append({_SYS_TRAIT_INDEX: trait_index,  # used as column index in the heatmap
                                _SYS_TRAIT_VALUE: trait_value,
                                loader_common_names.DETECTED_GENE_SCORE_COL: detected_gene_score,
                                })


def _create_import_files(root_dir: str, file_name: str, docs: list[dict[str, Any]]):
    # create and save the data documents as JSONLines file to the import directory

    import_dir = os.path.join(root_dir, IMPORT_DIR)
    os.makedirs(import_dir, exist_ok=True)

    file_path = os.path.join(import_dir, file_name)
    print(f'Creating JSONLines import file: {file_path}')
    with open(file_path, 'w') as f:
        convert_to_json(docs, f)


def _process_heatmap_tools(heatmap_tools: set[str],
                           root_dir: str,
                           kbase_collection: str,
                           load_ver: str):
    # parse result files generated by heatmap tools such as microtrait

    for tool in heatmap_tools:
        try:
            parse_ops = getattr(sys.modules[__name__], tool)
        except AttributeError as e:
            raise ValueError(f'Please implement parsing method for: [{tool}]') from e

        heatmap_meta_dict, heatmap_rows_list, heatmap_cell_details_list = parse_ops(
            root_dir, kbase_collection, load_ver)

        meta_output = f'{kbase_collection}_{load_ver}_{tool}_meta_{HEATMAP_FILE_SUFFIX}'
        rows_output = f'{kbase_collection}_{load_ver}_{tool}_rows_{HEATMAP_FILE_SUFFIX}'
        cell_details_output = f'{kbase_collection}_{load_ver}_{tool}_cell_details_{HEATMAP_FILE_SUFFIX}'

        _create_import_files(root_dir, meta_output, [heatmap_meta_dict])
        _create_import_files(root_dir, rows_output, heatmap_rows_list)
        _create_import_files(root_dir, cell_details_output, heatmap_cell_details_list)


def _process_genome_attri_tools(genome_attr_tools: set[str],
                                root_dir: str,
                                kbase_collection: str,
                                load_ver: str):
    # parse result files generated by genome attribute tools such as checkm2, gtdb-tk, etc

    if not genome_attr_tools:
        return

    genome_attr_tools = sorted(genome_attr_tools)  # sort the tools to ensure consistent order of the output
    docs = list()
    for tool in genome_attr_tools:
        try:
            parse_ops = getattr(sys.modules[__name__], tool)
        except AttributeError as e:
            raise ValueError(f'Please implement parsing method for: [{tool}]') from e

        docs.extend(parse_ops(root_dir, kbase_collection, load_ver))

    docs = merge_docs(docs, '_key')
    res_dict = {row[names.FLD_KBASE_ID]: row for row in docs}
    meta_lookup = _create_meta_lookup(root_dir, kbase_collection, load_ver, tool)
    docs, encountered_types = _update_docs_with_upa_info(res_dict, meta_lookup)

    output = f'{kbase_collection}_{load_ver}_{"_".join(genome_attr_tools)}_{COMPUTED_GENOME_ATTR_FILE_SUFFIX}'
    _create_import_files(root_dir, output, docs)

    export_types_output = f'{kbase_collection}_{load_ver}_{KBCALL_EXPORT_TYPES_FILE_SUFFIX}'
    types_doc = data_product_export_types_to_doc(kbase_collection, loader_common_names.GENOME_ATTRIBS, load_ver, sorted(encountered_types))
    _create_import_files(root_dir, export_types_output, [types_doc])


def _create_meta_lookup(root_dir, kbase_collection, load_ver, tool):
    # Create a hashmap with genome id as the key and metafile name as the value
    
    meta_lookup = {}
    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, tool=tool)
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


def _append_or_check_trait(
        global_traits_meta: dict[str, dict[str, str]],
        trait_id: str,
        trait_index: int,
        trait_name: str,
        trait_description: str,
        trait_category: str,
        trait_type: str):
    # Append a new trait to the global traits dictionary or check if the trait information is consistent

    if trait_id not in global_traits_meta:
        # add new trait
        global_traits_meta[trait_id] = {
            _SYS_TRAIT_INDEX: trait_index,
            _SYS_TRAIT_NAME: trait_name,
            _SYS_TRAIT_DESCRIPTION: trait_description,
            _SYS_TRAIT_CATEGORY: trait_category,
            _SYS_TRAIT_TYPE: trait_type,
        }
    else:
        # check if the trait information is consistent
        existing_trait = global_traits_meta[trait_id]
        if (existing_trait[_SYS_TRAIT_NAME] != trait_name or
                existing_trait[_SYS_TRAIT_INDEX] != trait_index or
                existing_trait[_SYS_TRAIT_DESCRIPTION] != trait_description or
                existing_trait[_SYS_TRAIT_CATEGORY] != trait_category or
                existing_trait[_SYS_TRAIT_TYPE] != trait_type):
            raise ValueError(f'Inconsistent trait information for trait {trait_id}')


def _parse_categories(traits_meta: dict[str, dict[str, str]]) -> list[ColumnCategory]:
    # Parse the trait categories from the global traits dictionary

    categories = {}
    # loop over each trait in traits_meta
    for trait in traits_meta.values():
        trait_category = trait[_SYS_TRAIT_CATEGORY]

        if trait[_SYS_TRAIT_TYPE] == 'count':
            trait_type = ColumnType.COUNT.value
        elif trait[_SYS_TRAIT_TYPE] == 'binary':
            trait_type = ColumnType.BOOL.value
        else:
            raise ValueError(f'Unknown trait type {trait[_SYS_TRAIT_TYPE]}')

        # if the trait_category is not available, create a new ColumnCategory object with empty columns
        if trait_category not in categories:
            categories[trait_category] = ColumnCategory(category=trait_category, columns=[])

        # add the trait to the appropriate category's list of columns
        categories[trait_category].columns.append(ColumnInformation(
            col_id=str(trait[_SYS_TRAIT_INDEX]),
            name=trait[_SYS_TRAIT_NAME],
            description=trait[_SYS_TRAIT_DESCRIPTION],
            type=trait_type
        ))

    # sort columns in each ColumnCategory object by the column id
    for category in categories.values():
        category.columns = sorted(category.columns, key=lambda column: int(column.col_id))

    # sort ColumnCategory objects by the column id of the first column in each ColumnCategory object
    sorted_categories = sorted(
        categories.values(), key=lambda category: int(category.columns[0].col_id)
    )

    # this is to ensure that the column ids are in ascending order
    # TODO: might need to skip this step for heatmap products other than microtrait
    column_ids = [column.col_id for category in sorted_categories for column in category.columns]
    if not _ensure_list_ordered(column_ids):
        raise ValueError(f'Column ids are not ordered in ascending order: {column_ids}')

    return sorted_categories


def _is_float_int(num: Any) -> bool:
    # Check if a number is an integer or a float with an integer value
    return isinstance(num, int) or (isinstance(num, float) and num.is_integer())


def _int(num: float | int) -> int:
    # Convert a float to an integer if the float is an integer
    if _is_float_int(num):
        return int(num)
    else:
        raise ValueError(f'Input must be an integer. Got {num} instead.')


def _is_binary(num: int) -> bool:
    # Given a number, checks if it is a binary num (i.e., being only 0 or 1).
    return num == 0 or num == 1


def _num_to_bool(num: int | bool) -> bool:
    # Given a number, checks if it is a binary type (i.e., contains only 0s and 1s).

    if _is_binary(num):
        return bool(num)
    else:
        raise ValueError(f'Input must be a binary number (i.e., 0 or 1). Got {num} instead.')


def _create_cell_detail(
        cell_id: str,
        detected_genes_score: dict[str, float]
) -> CellDetail:
    if not detected_genes_score:
        detected_genes_score = dict()

    return CellDetail(
        cell_id=cell_id,
        values=[
            CellDetailEntry(id=gene_name, val=gene_score)
            for gene_name, gene_score in detected_genes_score.items()
        ]
    )


def _append_cell(
        heatmap_row: HeatMapRow,
        trait_idx: int,
        cell_count: int,
        trait_type: str,
        min_value: float | int,
        max_value: float | int,
        trait_val: float | int | bool = _SYS_DEFAULT_TRAIT_VALUE,
        detected_genes_score: dict[str, float] = None,
) -> (float | int, float | int, CellDetail):
    # Append a cell to the heatmap row and return the global min and max values

    if trait_type == 'count':
        trait_val = _int(trait_val)
    elif trait_type == 'binary':
        trait_val = _num_to_bool(trait_val)
    else:
        raise ValueError(f'Unknown trait type {trait_type}')

    cell_id = str(cell_count)
    cell = Cell(cell_id=cell_id, col_id=str(trait_idx), val=trait_val)
    heatmap_row.cells.append(cell)

    cell_detail = _create_cell_detail(cell_id, detected_genes_score)

    if isinstance(trait_val, Number):
        min_value = min(min_value, trait_val)
        max_value = max(max_value, trait_val)

    return min_value, max_value, cell_detail


def _find_trait_by_index(
        trait_index: int,
        trait_meta: dict[str, dict[str, int | str]]
) -> dict[str, int | str]:
    # Find a trait in the global traits dictionary by its ID
    for trait in trait_meta.values():
        if trait[_SYS_TRAIT_INDEX] == trait_index:
            return trait

    raise ValueError(f'Unable to find trait with ID {trait_index}')


def _ensure_list_ordered(a_list: list[str]) -> bool:
    # Given a list of int strings, check if the list is ordered in ascending order
    return a_list == sorted(a_list, key=int)


def _parse_heatmap_rows(
        traits_meta: dict[str, dict[str, int | str]],
        traits_val: dict[str, list[dict[str, int | float | bool | dict[str, float]]]]
) -> (list[HeatMapRow], float | int, float | int):
    min_value, max_value, cell_count = float('inf'), float('-inf'), 0
    heatmap_rows, heatmap_cell_details = list(), list()
    trait_idxs = set([trait[_SYS_TRAIT_INDEX] for trait in traits_meta.values()])
    for data_id, traits_val_list in traits_val.items():
        heatmap_row = HeatMapRow(kbase_id=data_id, cells=[])
        visited_traits = set()
        for trait_val_info in traits_val_list:
            trait_idx = trait_val_info[_SYS_TRAIT_INDEX]
            trait_val = trait_val_info[_SYS_TRAIT_VALUE]
            detected_genes_score = trait_val_info[loader_common_names.DETECTED_GENE_SCORE_COL]
            trait = _find_trait_by_index(trait_idx, traits_meta)
            trait_type = trait.get(_SYS_TRAIT_TYPE)

            min_value, max_value, cell_detail = _append_cell(heatmap_row,
                                                             trait_idx,
                                                             cell_count,
                                                             trait_type,
                                                             min_value,
                                                             max_value,
                                                             trait_val=trait_val,
                                                             detected_genes_score=detected_genes_score)
            heatmap_cell_details.append(cell_detail)
            visited_traits.add(trait_idx)
            cell_count += 1

        # fill in missing trait values with 0s for all cells (happens when a trait is not present for a given data id)
        # In case of Microtriat, this should never happen.
        missing_trait_idxs = trait_idxs - visited_traits
        if missing_trait_idxs:
            print(f'Warning: missing trait values {missing_trait_idxs} for {data_id}.'
                  f' Filling in with {_SYS_DEFAULT_TRAIT_VALUE}s for cell value.')
        for missing_trait_idx in missing_trait_idxs:
            trait = _find_trait_by_index(missing_trait_idx, traits_meta)
            trait_type = trait.get(_SYS_TRAIT_TYPE)
            min_value, max_value, cell_detail = _append_cell(heatmap_row,
                                                             missing_trait_idx,
                                                             cell_count,
                                                             trait_type,
                                                             min_value,
                                                             max_value)
            heatmap_cell_details.append(cell_detail)
            cell_count += 1

        # sort the cells by column ID to ensure the heatmap is in the correct order
        heatmap_row.cells = sorted(heatmap_row.cells, key=lambda cell: int(cell.col_id))

        heatmap_rows.append(heatmap_row)

    return heatmap_rows, heatmap_cell_details, min_value, max_value


def _create_heatmap_objs(
        traits_meta: dict[str, dict[str, str]],
        traits_val: dict[str, list[dict[str, int | float | bool]]]
) -> (HeatMapMeta, list[HeatMapRow], list[CellDetail]):
    # Create the HeatMapMeta and list of HeatMapRow from parsed trait metadata and values

    heatmap_rows, heatmap_cell_details, min_value, max_value = _parse_heatmap_rows(traits_meta, traits_val)
    categories = _parse_categories(traits_meta)
    heatmap_meta = HeatMapMeta(categories=categories, min_value=min_value, max_value=max_value)

    return heatmap_meta, heatmap_rows, heatmap_cell_details


def _process_rows_list(rows_list, kbase_collection, load_ver, key_name, key_func):
    # Add the collection, load version and key to each row in the list
    return [dict(row, **{
        names.FLD_ARANGO_KEY: key_func(kbase_collection, load_ver, row[key_name]),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_ver}) for row in rows_list]


def microtrait(root_dir, kbase_collection, load_ver):
    """
    Parse and formate result files as heatmap data generated by the MicroTrait tool.
    """

    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, tool='microtrait')
    batch_dirs = _get_batch_dirs(result_dir)

    traits_meta, traits_val = dict(), dict()
    for batch_dir in batch_dirs:
        data_ids = [item for item in os.listdir(os.path.join(result_dir, batch_dir)) if
                    os.path.isdir(os.path.join(result_dir, batch_dir, item))]
        for data_id in data_ids:
            data_dir = os.path.join(result_dir, batch_dir, data_id)
            trait_count_file = os.path.join(data_dir, loader_common_names.TRAIT_COUNTS_FILE)
            selected_cols = _MICROTRAIT_TO_SYS_TRAIT_MAP.keys()
            trait_df = pd.read_csv(trait_count_file, usecols=selected_cols)

            # Check if the trait index column has non-unique values
            if len(trait_df[loader_common_names.MICROTRAIT_TRAIT_NAME].unique()) != len(trait_df):
                raise ValueError(f"The {loader_common_names.MICROTRAIT_TRAIT_NAME} column has non-unique values")

            # Extract the substring of the 'microtrait_trait-displaynamelong' column before the first colon character
            # and assign it to a new 'category' column in the DataFrame
            trait_df[_SYS_TRAIT_CATEGORY] = trait_df[_MICROTRAIT_TRAIT_DISPLAYNAME_LONG].str.split(':').str[0]

            trait_df = trait_df.rename(columns=_MICROTRAIT_TO_SYS_TRAIT_MAP)
            trait_df.apply(_process_trait, args=(traits_meta, traits_val, data_id), axis=1)

    heatmap_meta, heatmap_rows, heatmap_cell_details = _create_heatmap_objs(traits_meta, traits_val)
    heatmap_meta_dict = heatmap_meta.dict()
    heatmap_cell_details_list = [cell_detail.dict() for cell_detail in heatmap_cell_details]

    # Add _key, collection id and load version to the heatmap metadata and rows
    heatmap_meta_dict.update({
        names.FLD_ARANGO_KEY: collection_load_version_key(kbase_collection, load_ver),
        names.FLD_COLLECTION_ID: kbase_collection,
        names.FLD_LOAD_VERSION: load_ver
    })

    heatmap_rows_list = []
    for r in heatmap_rows:
        d = r.dict()
        d.pop(names.FLD_MATCHED, None)   # inserted by the model but not needed in the DB
        d.pop(names.FLD_SELECTED, None)  # inserted by the model but not needed in the DB
        heatmap_rows_list.append(dict(
            # Needs to have the match and selection field inserted
            d, **init_genome_atrri_doc(kbase_collection, load_ver, d[names.FLD_KBASE_ID])
        ))
    heatmap_cell_details_list = _process_rows_list(heatmap_cell_details_list,
                                                   kbase_collection,
                                                   load_ver,
                                                   FIELD_HEATMAP_CELL_ID,
                                                   collection_data_id_key)

    return heatmap_meta_dict, heatmap_rows_list, heatmap_cell_details_list


def gtdb_tk(root_dir, kbase_collection, load_ver):
    """
    Parse and format result files generated by the GTDB-TK tool.
    """
    gtdb_tk_docs = list()

    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, tool='gtdb_tk')
    batch_dirs = _get_batch_dirs(result_dir)

    summary_files = ['gtdbtk.ar53.summary.tsv', 'gtdbtk.bac120.summary.tsv']
    genome_id_col = 'user_genome'
    for batch_dir in batch_dirs:
        summary_file_exists = False

        for tool_file_name in summary_files:
            docs = _read_tool_result(result_dir, batch_dir, kbase_collection, load_ver,
                                     tool_file_name, SELECTED_GTDBTK_SUMMARY_FEATURES, genome_id_col)

            if docs:
                summary_file_exists = True
                gtdb_tk_docs.extend(docs)

        if not summary_file_exists:
            raise ValueError(f'Unable to process the computed genome attributes for gtdb-tk in the specified '
                             f'directory {batch_dir}.')

    return gtdb_tk_docs


def checkm2(root_dir, kbase_collection, load_ver):
    """
    Parse and formate result files generated by the CheckM2 tool.
    """
    checkm2_docs = list()

    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, tool='checkm2')
    batch_dirs = _get_batch_dirs(result_dir)

    tool_file_name, genome_id_col = 'quality_report.tsv', 'Name'
    for batch_dir in batch_dirs:
        docs = _read_tool_result(result_dir, batch_dir, kbase_collection, load_ver,
                                 tool_file_name, SELECTED_CHECKM2_FEATURES, genome_id_col)

        if not docs:
            raise ValueError(f'Unable to process the computed genome attributes for checkm2 in the specified '
                             f'directory {batch_dir}.')

        checkm2_docs.extend(docs)

    return checkm2_docs


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Generate a JSON file for importing into ArangoDB by parsing computed '
                    'genome attributes.')
    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required flag arguments
    required.add_argument(f'--{loader_common_names.LOAD_VER_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.LOAD_VER_DESCR)

    required.add_argument(f'--{loader_common_names.KBASE_COLLECTION_ARG_NAME}', required=True, type=str,
                          help=loader_common_names.KBASE_COLLECTION_DESCR)

    # Optional arguments
    optional.add_argument('--tools', type=str, nargs='+',
                          help=f'Extract results from tools. '
                               f'(default: retrieve all available sub-directories in the '
                               f'[{loader_common_names.LOAD_VER_ARG_NAME}] directory)')
    optional.add_argument('--root_dir', type=str, default=loader_common_names.ROOT_DIR,
                          help=f'Root directory for the collections project. (default: {loader_common_names.ROOT_DIR})')
    args = parser.parse_args()

    tools = args.tools
    load_ver = getattr(args, loader_common_names.LOAD_VER_ARG_NAME)
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)
    root_dir = args.root_dir

    result_dir = _locate_dir(root_dir, kbase_collection, load_ver, check_exists=True)

    executed_tools = [d for d in os.listdir(result_dir) if os.path.isdir(os.path.join(result_dir, d))]
    if not executed_tools:
        raise ValueError(f'Cannot find any tool result folders in {result_dir}')

    tools = executed_tools if not tools else tools
    if set(tools) - set(executed_tools):
        raise ValueError(f'Please ensure that all tools have been successfully executed. '
                         f'Only the following tools have already been run: {executed_tools}')

    _process_genome_attri_tools(set(GENOME_ATTR_TOOLS).intersection(tools), root_dir, kbase_collection, load_ver)
    _process_heatmap_tools(set(HEATMAP_TOOLS).intersection(tools), root_dir, kbase_collection, load_ver)


if __name__ == "__main__":
    main()
