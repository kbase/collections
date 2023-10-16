"""
Runs microtrait on a set of assemblies.
"""
import os
import uuid
from pathlib import Path
from typing import Any

import pandas as pd
from rpy2 import robjects

from src.common.product_models.heatmap_common_models import (
    FIELD_HEATMAP_VALUES,
    FIELD_HEATMAP_ROW_CELLS,
    FIELD_HEATMAP_CELL_ID,
    FIELD_HEATMAP_COL_ID,
    FIELD_HEATMAP_CELL_VALUE,
    FIELD_HEATMAP_NAME,
    FIELD_HEATMAP_DESCR,
    FIELD_HEATMAP_TYPE,
    FIELD_HEATMAP_CATEGORY,
    ColumnType)
from src.common.storage.field_names import FLD_KBASE_ID
from src.loaders.common import loader_common_names
from src.loaders.compute_tools.tool_common import (
    FatalTuple,
    ToolRunner,
    write_fatal_tuples_to_dict,
)
from src.loaders.compute_tools.tool_result_parser import (
    create_jsonl_files,
    MICROTRAIT_META,
    MICROTRAIT_CELLS,
    MICROTRAIT_DATA)

# the name of the component used for extracting traits from microtrait's 'extract.traits' result
TRAIT_COUNTS_ATGRANULARITY = 'trait_counts_atgranularity3'
_GENE_NAME_COL = 'hmm_name'  # column name from the genes_detected_table file that contains the gene name
_GENE_SCORE_COL = 'gene_score'  # column name from the genes_detected_table file that contains the gene score

# The following features will be extracted from the MicroTrait result file as heatmap data
_MICROTRAIT_TRAIT_DISPLAYNAME_SHORT = 'microtrait_trait-displaynameshort'  # used as column name of the trait
_MICROTRAIT_TRAIT_DISPLAYNAME_LONG = 'microtrait_trait-displaynamelong'  # used as description of the trait
_MICROTRAIT_TRAIT_VALUE = 'microtrait_trait-value'  # value of the trait (can be integer or 0/1 as boolean)
_MICROTRAIT_TRAIT_TYPE = 'microtrait_trait-type'  # type of trait (count or binary)
_MICROTRAIT_TRAIT_ORDER = 'microtrait_trait-displayorder'  # order of the trait defined by the granularity table used as the index of trait
_MICROTRAIT_TRAIT_NAME = 'microtrait_trait-name'  # column name for the trait unique identifier defined in the granularity trait count table

_SYS_DEFAULT_TRAIT_VALUE = 0  # default value (0 or False) for a trait if the value is missing/not available

_DETECTED_GENE_SCORE_COL = 'detected_genes_score'  # column name for the detected genes score

# The map between the MicroTrait trait names and the corresponding system trait names
# Use the microtrait_trait-name column as the unique identifier for a trait globally,
# the microtrait_trait-displaynameshort column as the column name,
# microtrait_trait-displaynamelong column as the column description, and
# microtrait_trait-value as the cell value
_MICROTRAIT_TO_SYS_TRAIT_MAP = {
    _MICROTRAIT_TRAIT_NAME: loader_common_names.SYS_TRAIT_ID,
    _MICROTRAIT_TRAIT_DISPLAYNAME_SHORT: FIELD_HEATMAP_NAME,
    _MICROTRAIT_TRAIT_DISPLAYNAME_LONG: FIELD_HEATMAP_DESCR,
    _MICROTRAIT_TRAIT_VALUE: FIELD_HEATMAP_CELL_VALUE,
    _MICROTRAIT_TRAIT_TYPE: FIELD_HEATMAP_TYPE,
    _MICROTRAIT_TRAIT_ORDER: FIELD_HEATMAP_COL_ID,
    _DETECTED_GENE_SCORE_COL: _DETECTED_GENE_SCORE_COL,
}


def _get_r_list_element(r_list, element_name):
    # retrieve the element from the R list
    if element_name not in r_list.names:
        return None, False
    pos = r_list.names.index(element_name)
    return r_list[pos], True


def _r_table_to_df(r_table):
    # convert R table to pandas dataframe

    data = dict()
    for idx, name in enumerate(r_table.names):

        if isinstance(r_table[idx], robjects.vectors.FactorVector):
            levels = tuple(r_table[idx].levels)
            values = [levels[idx - 1] for idx in tuple(r_table[idx])]
            data.update({name: values})
        else:
            data.update({name: list(r_table[idx])})

    df = pd.DataFrame(data=data)

    return df


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


def _process_trait_counts(
        trait_counts_df: pd.DataFrame,
        data_id: str):
    # process the trait counts file to create the heatmap data and metadata

    trait_df = trait_counts_df[_MICROTRAIT_TO_SYS_TRAIT_MAP.keys()]

    # Check if the trait index column has non-unique values
    if len(trait_df[_MICROTRAIT_TRAIT_NAME].unique()) != len(trait_df):
        raise ValueError(f"The {_MICROTRAIT_TRAIT_NAME} column has non-unique values")

    # Extract the substring of the 'microtrait_trait-displaynamelong' column before the first colon character
    # and assign it to a new 'category' column in the DataFrame
    trait_df[FIELD_HEATMAP_CATEGORY] = trait_df[_MICROTRAIT_TRAIT_DISPLAYNAME_LONG].str.split(':').str[0]

    trait_df = trait_df.rename(columns=_MICROTRAIT_TO_SYS_TRAIT_MAP)

    # ensure the col_id column is string type
    trait_df[FIELD_HEATMAP_COL_ID] = trait_df[FIELD_HEATMAP_COL_ID].astype(str)

    traits = trait_df.to_dict(orient='records')
    cells, cells_meta, traits_meta = list(), list(), list()
    for trait in traits:

        # process trait info by different trait types
        if trait[FIELD_HEATMAP_TYPE] == 'count':
            trait[FIELD_HEATMAP_TYPE] = ColumnType.COUNT.value
            trait[FIELD_HEATMAP_CELL_VALUE] = _int(trait[FIELD_HEATMAP_CELL_VALUE])
        elif trait[FIELD_HEATMAP_TYPE] == 'binary':
            trait[FIELD_HEATMAP_TYPE] = ColumnType.BOOL.value
            trait[FIELD_HEATMAP_CELL_VALUE] = _num_to_bool(trait[FIELD_HEATMAP_CELL_VALUE])
        else:
            raise ValueError(f'Unknown trait type {trait[FIELD_HEATMAP_TYPE]}')

        cell_uuid = str(uuid.uuid4())
        # process cell data
        cells.append({FIELD_HEATMAP_CELL_ID: cell_uuid,
                      FIELD_HEATMAP_COL_ID: trait[FIELD_HEATMAP_COL_ID],
                      FIELD_HEATMAP_CELL_VALUE: trait[FIELD_HEATMAP_CELL_VALUE]})

        # process cell meta
        cells_meta.append({FIELD_HEATMAP_CELL_ID: cell_uuid,
                           FIELD_HEATMAP_VALUES: trait[_DETECTED_GENE_SCORE_COL]})

        # process trait meta
        trait_meta_keys = [FIELD_HEATMAP_COL_ID,
                           FIELD_HEATMAP_NAME,
                           FIELD_HEATMAP_DESCR,
                           FIELD_HEATMAP_CATEGORY,
                           FIELD_HEATMAP_TYPE]

        traits_meta.append({key: trait[key] for key in trait_meta_keys})

    # sort the traits_meta list by FIELD_HEATMAP_COL_ID
    traits_meta = sorted(traits_meta, key=lambda x: int(x[FIELD_HEATMAP_COL_ID]))

    # sort the cells list by FIELD_HEATMAP_COL_ID
    cells = sorted(cells, key=lambda x: int(x[FIELD_HEATMAP_COL_ID]))

    heatmap_row = [{FLD_KBASE_ID: data_id,
                    FIELD_HEATMAP_ROW_CELLS: cells}]

    return heatmap_row, cells_meta, traits_meta


def _run_microtrait(tool_safe_data_id: str, data_id: str, fna_file: Path, genome_dir: Path, debug: bool):
    # run microtrait.extract_traits on the genome file
    # https://github.com/ukaraoz/microtrait

    # result files:
    #   * An RDS file created by the microtrait.extract_traits function. The RDS files from all
    #     genomes can be
    # used to build the trait matrices and hmm matrix.
    #   * A genes_detected_table file (CSV format) retrieved from the result microtrait_result
    #     object returned by the
    #     extract_traits function.

    # Load the R script as an R function
    r_script = """
        library(microtrait)
        extract_traits <- function(genome_file, out_dir) {
            genome_file <- file.path(genome_file)
            microtrait_result <- extract.traits(in_file = genome_file, out_dir = out_dir)
            return(microtrait_result)
        }
    """
    r_func = robjects.r(r_script)
    r_result = r_func(str(fna_file), str(genome_dir))

    microtrait_result, _ = _get_r_list_element(r_result, 'microtrait_result')

    trait_counts, exist = _get_r_list_element(microtrait_result, TRAIT_COUNTS_ATGRANULARITY)
    if not exist:
        error_message = "Microtrait output no data"
        fatal_tuples = [FatalTuple(data_id, error_message, str(fna_file), None)]
        write_fatal_tuples_to_dict(fatal_tuples, genome_dir)
        return
    # example trait_counts_df from trait_counts_atgranularity3
    # microtrait_trait-name,microtrait_trait-value,microtrait_trait-displaynameshort,microtrait_trait-displaynamelong,microtrait_trait-strategy,microtrait_trait-type,microtrait_trait-granularity,microtrait_trait-version,microtrait_trait-displayorder,microtrait_trait-value1
    # Resource Acquisition:Substrate uptake:aromatic acid transport,1,Aromatic acid transport,Resource Acquisition:Substrate uptake:aromatic acid transport,Resource Acquisition,count,3,production,1,1
    # Resource Acquisition:Substrate uptake:biopolymer transport,3,Biopolymer transport,Resource Acquisition:Substrate uptake:biopolymer transport,Resource Acquisition,count,3,production,2,3
    trait_counts_df = _r_table_to_df(trait_counts)

    trait_unwrapped_rules_file = os.environ.get('MT_TRAIT_UNWRAPPED_FILE')
    if trait_unwrapped_rules_file:
        trait_unwrapped_rules_df = pd.read_csv(trait_unwrapped_rules_file, sep='\t')
        trait_counts_df = trait_counts_df.merge(trait_unwrapped_rules_df,
                                                left_on=_MICROTRAIT_TRAIT_NAME,
                                                right_on=loader_common_names.SYS_TRAIT_ID,
                                                how='left')
        trait_counts_df.drop(columns=[loader_common_names.SYS_TRAIT_ID], inplace=True)

        genes_detected_table, _ = _get_r_list_element(microtrait_result, 'genes_detected_table')
        genes_detected_df = _r_table_to_df(genes_detected_table)
        detected_genes_score = dict(zip(genes_detected_df[_GENE_NAME_COL], genes_detected_df[_GENE_SCORE_COL]))

        trait_counts_df[_DETECTED_GENE_SCORE_COL] = trait_counts_df[
            loader_common_names.UNWRAPPED_GENE_COL].apply(
            lambda x: [{gene: detected_genes_score.get(gene) for gene in str(x).split(';') if
                       gene in detected_genes_score}])
    else:
        raise ValueError('Please set environment variable MT_TRAIT_UNWRAPPED_FILE')

    trait_counts_df.to_csv(os.path.join(genome_dir, loader_common_names.TRAIT_COUNTS_FILE), index=False)

    heatmap_row, cells_meta, traits_meta = _process_trait_counts(trait_counts_df, data_id)
    create_jsonl_files(genome_dir / MICROTRAIT_META, traits_meta)
    create_jsonl_files(genome_dir / MICROTRAIT_CELLS, cells_meta)
    create_jsonl_files(genome_dir / MICROTRAIT_DATA, heatmap_row)


def main():
    runner = ToolRunner("microtrait")
    runner.parallel_single_execution(_run_microtrait, unzip=True)


if __name__ == "__main__":
    main()
