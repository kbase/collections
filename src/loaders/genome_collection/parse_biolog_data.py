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
    FIELD_HEATMAP_TYPE,
    FIELD_HEATMAP_VALUES,
    FIELD_HEATMAP_CATEGORY,
    FIELD_HEATMAP_CATEGORIES,
)
from src.common.storage.collection_and_field_names import (
    COLL_MICROTRAIT_CELLS,
    COLL_MICROTRAIT_DATA,
    COLL_MICROTRAIT_META,
    FLD_ARANGO_KEY,
    FLD_COLLECTION_ID,
    FLD_LOAD_VERSION,
)
from src.common.storage.db_doc_conversions import collection_load_version_key
from src.common.storage.field_names import FLD_KBASE_ID
from src.loaders.common import loader_common_names
from src.loaders.common.loader_helper import init_row_doc, create_import_files
from src.loaders.genome_collection.parse_tool_results import HEATMAP_FILE_ROOT


def _read_excel_as_df(excel_file: Path, sheet_name=0) -> pd.DataFrame:
    # Read the Excel file into a DataFrame

    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        return df

    except FileNotFoundError:
        print(f"File '{excel_file}' not found.")


def _read_biolog_data(biolog_data_file: Path) -> pd.DataFrame:
    # Read the Biolog data file into a DataFrame and do some data cleaning

    data_df = _read_excel_as_df(biolog_data_file)

    # NOTE: below logic is specific to the Biolog data file (PMI strain BiologSummary.xlsx)
    # link to the file: https://docs.google.com/spreadsheets/d/1QmC6UHWOEVfpmrveRBl_izictbNeN1PA/edit#gid=1135979967

    # Drop unused columns
    data_df = data_df.drop(columns=['# pos. strains'])

    # Make the first row ('strain designation') the column names
    data_df.columns = data_df.loc[1]

    # Drop unused rows ('total cpds. Utilized', 'strain designation' and unused rows at the end.)
    data_df = data_df.drop(index=[0, 1, 194, 195, 196, 197, 198])

    # Make the first column ('strain designation') the index
    data_df = data_df.set_index('strain designation')

    # Transpose the DataFrame
    data_df.index.name = None
    data_df.columns.name = None
    data_df = data_df.transpose()

    # Remove asterisks from index names
    data_df.index = data_df.index.str.replace(r'*', '').str.replace(r'**', '')

    # Merge rows BT03 A(10/1) and BT03 B(11/18) into one row BT03
    data_df = data_df.drop(index='BT03 B(11/18)')
    data_df = data_df.rename(index={'BT03 A(10/1)': 'BT03'})

    # Replace NaN with 0
    # TODO: this is a temporary solution to handle the missing data for some strains.
    # Two NaN values are present in the data file for 'YR530' and 'CF286' in the row labeled '3-Hydroxy 2-Butanone'.
    data_df.fillna(0, inplace=True)

    return data_df


def _read_biolog_meta(biolog_meta_file: Path) -> pd.DataFrame:
    meta_df = _read_excel_as_df(biolog_meta_file)

    return meta_df


def _retrieve_kbase_genome_id(meta_df, strain_id) -> str:
    # Retrieve the KBase genome ID for the given strain ID from the metadata file (dataframe).

    matching_row = meta_df[(meta_df['Strain ID'] == strain_id) & meta_df['Genome_Ref'].notna()]

    if matching_row.empty:
        raise ValueError(f"No matching Strain ID with a non-NaN Genome_Ref found for {strain_id}.")
    else:
        genome_ref = matching_row['Genome_Ref'].values[0]

        return genome_ref


def generate_biolog_heatmap_data(
        biolog_data_file: Path,
        biolog_meta_file: Path,
        load_ver: str,
        env: str = 'PROD',
        kbase_collection: str = 'biolog',
        root_dir: str = loader_common_names.ROOT_DIR):
    """
    Generate JSONL files with the heatmap data for the Biolog data file.

    :param biolog_data_file: Biolog data file
    :param biolog_meta_file: Biolog meta file
    :param load_ver: the version of the data to be loaded
    :param env: the environment containing the data to be processed
    :param kbase_collection: the name of the KBase collection
    :param root_dir: the root directory for the data to be processed
    """
    data_df = _read_biolog_data(biolog_data_file)
    meta_df = _read_biolog_meta(biolog_meta_file)

    heatmap_cell_details, heatmap_rows, heatmap_meta_dict = list(), list(), dict()

    # create document for heatmap meta
    categories = list()
    for idx, metabolite in enumerate(data_df.columns):
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
            genome_ref = _retrieve_kbase_genome_id(meta_df, strain_id)
        except ValueError as e:
            # TODO: this is a temporary solution to handle the missing KBase genome ID for some strains.
            # Two strains 'PDO1076' and 'PTD-1' are missing the KBase genome ID in the metadata file.
            print(f'Cannot find KBase genome ID for strain ID: {strain_id}.')
            genome_ref = f'kbase_id_not_found - {str(uuid.uuid4())}'

        cells = list()
        for metabolite, value in row.items():
            cell_uuid, min_value, max_value = str(uuid.uuid4()), min(min_value, value), max(max_value, value)
            cells.append({FIELD_HEATMAP_CELL_ID: cell_uuid,
                          FIELD_HEATMAP_COL_ID: str(list(row.index).index(metabolite)),
                          FIELD_HEATMAP_CELL_VALUE: bool(value),
                          })
            # append a document for the heatmap cell details
            heatmap_cell_details.append({
                FIELD_HEATMAP_CELL_ID: cell_uuid,
                FIELD_HEATMAP_VALUES: dict(),  # 'values' for biolog is always empty
                FLD_COLLECTION_ID: kbase_collection,
                FLD_LOAD_VERSION: load_ver,
                FLD_ARANGO_KEY: cell_uuid,
            })

        # append a document for the heatmap rows
        heatmap_rows.append(dict({FLD_KBASE_ID: genome_ref,
                                  FIELD_HEATMAP_ROW_CELLS: cells},
                                 **init_row_doc(kbase_collection, load_ver, genome_ref)))

    heatmap_meta_dict[FIELD_HEATMAP_MIN_VALUE] = min_value
    heatmap_meta_dict[FIELD_HEATMAP_MAX_VALUE] = max_value

    tool = 'biolog'
    meta_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{COLL_MICROTRAIT_META}.jsonl'
    rows_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{COLL_MICROTRAIT_DATA}.jsonl'
    cell_details_output = f'{kbase_collection}_{load_ver}_{tool}_{HEATMAP_FILE_ROOT}_{COLL_MICROTRAIT_CELLS}.jsonl'

    create_import_files(root_dir, env, meta_output, [heatmap_meta_dict])
    create_import_files(root_dir, env, rows_output, heatmap_rows)
    create_import_files(root_dir, env, cell_details_output, heatmap_cell_details)
