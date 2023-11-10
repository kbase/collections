import json
import re
import uuid
from pathlib import Path

import pandas as pd

from src.common.product_models.heatmap_common_models import (
    ColumnType,
    FIELD_HEATMAP_CELL_ID,
    FIELD_HEATMAP_CELL_VALUE,
    FIELD_HEATMAP_COL_ID,
    FIELD_HEATMAP_COLUMNS,
    FIELD_HEATMAP_DESCR,
    FIELD_HEATMAP_MAX_VALUE,
    FIELD_HEATMAP_MIN_VALUE,
    FIELD_HEATMAP_NAME,
    FIELD_HEATMAP_ROW_CELLS,
    FIELD_HEATMAP_ROW_META,
    FIELD_HEATMAP_TYPE,
    FIELD_HEATMAP_VALUES,
    FIELD_HEATMAP_CATEGORY,
    FIELD_HEATMAP_CATEGORIES,
)
from src.common.storage.collection_and_field_names import (
    FLD_ARANGO_KEY,
    FLD_COLLECTION_ID,
    FLD_LOAD_VERSION,
    COLL_BIOLOG_META,
    COLL_BIOLOG_DATA,
    COLL_BIOLOG_CELLS,
)
from src.common.storage.db_doc_conversions import collection_load_version_key, collection_data_id_key
from src.common.storage.field_names import FLD_KBASE_ID
from src.loaders.common import loader_common_names
from src.loaders.common.loader_helper import init_row_doc, create_import_files
from src.loaders.genome_collection.parse_tool_results import HEATMAP_FILE_ROOT

GROWTH_MEDIA_COL_NAME = 'growth_media'
STRAIN_DESIGNATION_COL_NAME = 'strain designation'
DEFAULT_MEDIA = 'MOPS minimal media'


def _read_excel_as_df(excel_file: Path, sheet_name=0) -> pd.DataFrame:
    # Read the Excel file into a DataFrame

    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        return df
    except Exception as e:
        raise ValueError(f"Error reading Excel file '{excel_file}': {e}") from e


def _find_matching_media(column_name, media_mapping):
    # find the matching key from media_mapping
    for key in media_mapping.keys():
        if column_name.endswith(key):
            return media_mapping[key]
    return DEFAULT_MEDIA


def _read_biolog_data(biolog_data_file: Path) -> pd.DataFrame:
    # Read the Biolog data file into a DataFrame and do some data cleaning

    data_df = _read_excel_as_df(biolog_data_file)

    # NOTE: below logic is specific to the Biolog data file (PMI strain BiologSummary.xlsx)
    # link to the file: https://docs.google.com/spreadsheets/d/1QmC6UHWOEVfpmrveRBl_izictbNeN1PA/edit#gid=1135979967

    # Drop unused columns
    data_df = data_df.drop(columns=['# pos. strains'])

    # Make the first row ('strain designation') the column names
    data_df.columns = data_df.loc[1]

    # Iterate through the "strain designation" column and parse the growth media description
    media_mapping = {}
    for value in data_df[STRAIN_DESIGNATION_COL_NAME]:
        if isinstance(value, str):
            matches = re.findall(r'\*+', value)  # find '*' and '**' patterns
            if matches:
                num_stars = max(len(match) for match in matches)
                media_description = value.replace('*', '').strip()
                media_mapping['*' * num_stars] = media_description

    # Reorder media_mapping by the number of '*' characters in reverse order.
    # This step is necessary to ensure that _find_matching_media can match the longest key
    # because the keys ('*' and '**') are inclusive.
    media_mapping = dict(sorted(media_mapping.items(), key=lambda x: len(x[0]), reverse=True))

    # Set the growth media row with the matching media description
    data_df.loc[GROWTH_MEDIA_COL_NAME] = data_df.columns.map(lambda x: _find_matching_media(x, media_mapping))

    # Drop unused rows ('total cpds. Utilized', 'strain designation' and unused rows at the end.)
    # NOTE: Drop 'strain designation' here because it also becomes the column names at the previous step.
    #       Row 'Carbon Source' is used as the index upon reading the file.
    #       Therefore, dataframe row index 0 starts at 'total cpds. Utilized' and
    #       the last row index with data is 193 (row 195 '3-Hydroxy 2-Butanone' in the Excel sheet).
    data_df = data_df.drop(index=[0, 1, 194, 195, 196, 197, 198])

    # Switch the value of the cell in row `growth_media` and column `strain designation` from the default media string
    # to `growth_media`. This is needed so that when we make `strain designation` the index in the next step
    # `growth_media` will be a row in that index.
    data_df.loc[GROWTH_MEDIA_COL_NAME, STRAIN_DESIGNATION_COL_NAME] = GROWTH_MEDIA_COL_NAME
    # Make the first column ('strain designation') the index
    data_df = data_df.set_index(STRAIN_DESIGNATION_COL_NAME)

    # Transpose the DataFrame
    data_df = data_df.transpose()

    # Remove asterisks from index names
    data_df.index = data_df.index.str.replace(r'*', '').str.replace(r'**', '')

    # Drop the row BT03 A(10/1) and rename the row BT03 B(11/18) to BT03
    data_df = data_df.drop(index='BT03 A(10/1)')
    data_df = data_df.rename(index={'BT03 B(11/18)': 'BT03'})

    # Check for NA values in the DataFrame
    if data_df.isna().any(axis=1).any():
        raise ValueError(f"Some of the strains in the Biolog data exhibit missing values: "
                         f"{data_df.index[data_df.isna().any(axis=1)].tolist()}")

    return data_df


def _read_biolog_meta(biolog_meta_file: Path) -> pd.DataFrame:
    meta_df = _read_excel_as_df(biolog_meta_file)

    return meta_df


def _retrieve_kbase_assembly_id(meta_df, strain_id) -> str:
    # Retrieve the KBase Assembly Ref for the given strain ID from the metadata file (dataframe).

    matching_row = meta_df[(meta_df['Strain ID'] == strain_id) & meta_df['Assembly ref'].notna()]

    if matching_row.empty:
        raise ValueError(f"No matching Strain ID with a non-NaN Assembly ref found for {strain_id}.")
    else:
        assembly_ref = matching_row['Assembly ref'].values[0]

        return assembly_ref


def _read_upa_mapping(upa_map_file: Path) -> dict:
    # Read the UPA mapping file into a dictionary

    with open(upa_map_file, 'r') as json_file:
        upa_mapping = json.load(json_file)

    return upa_mapping


def generate_pmi_biolog_heatmap_data(
        biolog_data_file: Path,
        biolog_meta_file: Path,
        load_ver: str,
        env: str = 'PROD',
        kbase_collection: str = 'PMI',
        root_dir: str = loader_common_names.ROOT_DIR,
        upa_map_file: Path = None, ):
    """
    Generate JSONL files with the heatmap data for the Biolog data file.

    :param biolog_data_file: Biolog data file, e.g. 'PMI strain BiologSummary.xlsx'
    :param biolog_meta_file: Biolog meta file, e.g. 'genome_assembly_info__PMI_metadata_file_all_strains_table001.xlsx'
    :param load_ver: the version of the data to be loaded
    :param env: the environment containing the data to be processed
    :param kbase_collection: the name of the KBase collection
    :param root_dir: the root directory for the data to be processed
    :param upa_map_file: the path to the JSON file containing the mapping between KBase PROD UPA to other environment UPA
    """
    if env != 'PROD' and upa_map_file is None:
        raise ValueError('upa_map_file must be provided for environment other than PROD.')

    data_df = _read_biolog_data(biolog_data_file)
    meta_df = _read_biolog_meta(biolog_meta_file)
    upa_mapping = _read_upa_mapping(upa_map_file) if env != 'PROD' else dict()

    heatmap_cell_details, heatmap_rows, heatmap_meta_dict = list(), list(), dict()

    # NOTE: If we ever need to modify the logic to accommodate changes in the heatmap data structure,
    # it's probable that we will also need to make corresponding updates to the logic in parse_tool_result.py.

    # create document for heatmap meta
    categories = list()
    for idx, metabolite in enumerate(data_df.columns):
        if metabolite == GROWTH_MEDIA_COL_NAME:
            continue
        # making each metabolite a category
        categories.append({FIELD_HEATMAP_CATEGORY: metabolite,
                           FIELD_HEATMAP_COLUMNS: [{FIELD_HEATMAP_COL_ID: str(idx),
                                                    FIELD_HEATMAP_NAME: metabolite,
                                                    FIELD_HEATMAP_DESCR: metabolite,
                                                    FIELD_HEATMAP_TYPE: ColumnType.BOOL.value
                                                    }]
                           })
    heatmap_meta_dict[FIELD_HEATMAP_CATEGORIES] = categories
    heatmap_meta_dict[FLD_COLLECTION_ID] = kbase_collection
    heatmap_meta_dict[FLD_LOAD_VERSION] = load_ver
    heatmap_meta_dict[FLD_ARANGO_KEY] = collection_load_version_key(kbase_collection, load_ver)

    min_value, max_value = float('inf'), float('-inf')
    for strain_id, row in data_df.iterrows():
        try:
            prod_assembly_ref = _retrieve_kbase_assembly_id(meta_df, strain_id)
        except ValueError as e:
            # TODO: this is a temporary solution to handle the missing KBase genome ID for some strains.
            # Two strains 'PDO1076' and 'PTD-1' are missing the KBase assembly ID in the metadata file.
            print(f'Cannot find KBase assembly ID for strain ID: {strain_id}. Skipping this row.')
            continue

        assembly_ref = upa_mapping.get(prod_assembly_ref) if env != 'PROD' else prod_assembly_ref

        if env != 'PROD' and assembly_ref is None:
            raise ValueError(f'Cannot find UPA mapping for {prod_assembly_ref}')

        cells = list()
        for metabolite, value in row.items():
            if metabolite == GROWTH_MEDIA_COL_NAME:
                continue
            cell_uuid, min_value, max_value = str(uuid.uuid4()), min(min_value, value), max(max_value, value)
            cells.append({FIELD_HEATMAP_CELL_ID: cell_uuid,
                          FIELD_HEATMAP_COL_ID: str(list(row.index).index(metabolite)),
                          FIELD_HEATMAP_CELL_VALUE: bool(value),
                          })
            # append a document for the heatmap cell details
            heatmap_cell_details.append({
                FIELD_HEATMAP_CELL_ID: cell_uuid,
                FIELD_HEATMAP_VALUES: list(),  # 'values' for biolog is always empty
                FLD_COLLECTION_ID: kbase_collection,
                FLD_LOAD_VERSION: load_ver,
                FLD_ARANGO_KEY: collection_data_id_key(kbase_collection, load_ver, cell_uuid),
            })

        # append a document for the heatmap rows
        heatmap_rows.append(dict({FLD_KBASE_ID: assembly_ref,
                                  FIELD_HEATMAP_ROW_CELLS: cells,
                                  FIELD_HEATMAP_ROW_META: {'growth_media': row[GROWTH_MEDIA_COL_NAME], }},
                                 **init_row_doc(kbase_collection, load_ver, assembly_ref)))

    heatmap_meta_dict[FIELD_HEATMAP_MIN_VALUE] = min_value
    heatmap_meta_dict[FIELD_HEATMAP_MAX_VALUE] = max_value

    tool = 'biolog'
    meta_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{COLL_BIOLOG_META}.jsonl'
    rows_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{COLL_BIOLOG_DATA}.jsonl'
    cell_details_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{COLL_BIOLOG_CELLS}.jsonl'

    create_import_files(root_dir, env, kbase_collection, load_ver, meta_output, [heatmap_meta_dict])
    create_import_files(root_dir, env, kbase_collection, load_ver, rows_output, heatmap_rows)
    create_import_files(root_dir, env, kbase_collection, load_ver, cell_details_output, heatmap_cell_details)
