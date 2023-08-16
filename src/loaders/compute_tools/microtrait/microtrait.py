"""
Runs microtrait on a set of assemblies.
"""
import json
import os
import uuid
from pathlib import Path

import pandas as pd
from rpy2 import robjects

from src.common.product_models.field_names import (
    FIELD_HEATMAP_KBASE_ID,
    FIELD_HEATMAP_VALUES,
    FIELD_HEATMAP_ROW_CELLS,
    FIELD_HEATMAP_CELL_ID,
    FIELD_HEATMAP_COL_ID,
    FIELD_HEATMAP_CELL_VALUE)
from src.common.storage.collection_and_field_names import COLL_MICROTRAIT_META
from src.loaders.common import loader_common_names
from src.loaders.compute_tools.tool_common import (
    FatalTuple,
    ToolRunner,
    write_fatal_tuples_to_dict,
)
from src.loaders.compute_tools.tool_result_parser import create_jsonl_files, MICROTRAIT_META, MICROTRAIT_CELLS, \
    MICROTRAIT_DATA

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


def _process_trait_counts(
        trait_counts_df: pd.DataFrame,
        data_id: str):
    # process the trait counts file to create the heatmap data and metadata

    trait_df = trait_counts_df[_MICROTRAIT_TO_SYS_TRAIT_MAP.keys()]

    # Check if the trait index column has non-unique values
    if len(trait_df[loader_common_names.MICROTRAIT_TRAIT_NAME].unique()) != len(trait_df):
        raise ValueError(f"The {loader_common_names.MICROTRAIT_TRAIT_NAME} column has non-unique values")

    # Extract the substring of the 'microtrait_trait-displaynamelong' column before the first colon character
    # and assign it to a new 'category' column in the DataFrame
    trait_df[_SYS_TRAIT_CATEGORY] = trait_df[_MICROTRAIT_TRAIT_DISPLAYNAME_LONG].str.split(':').str[0]

    trait_df = trait_df.rename(columns=_MICROTRAIT_TO_SYS_TRAIT_MAP)
    traits = trait_df.to_dict(orient='records')
    cells, cells_meta, traits_meta = list(), list(), list()
    for trait in traits:
        cell_uuid = str(uuid.uuid4())
        trait_val = trait[_SYS_TRAIT_VALUE]
        # process cell data
        cells.append({FIELD_HEATMAP_CELL_ID: cell_uuid,
                      FIELD_HEATMAP_COL_ID: trait[_SYS_TRAIT_INDEX],
                      FIELD_HEATMAP_CELL_VALUE: trait_val})

        # process cell meta
        detected_genes_score = trait[loader_common_names.DETECTED_GENE_SCORE_COL]
        cells_meta.append({FIELD_HEATMAP_CELL_ID: cell_uuid,
                           FIELD_HEATMAP_VALUES: detected_genes_score})

        # process trait meta
        trait_meta_keys = [_SYS_TRAIT_INDEX,
                           _SYS_TRAIT_NAME,
                           _SYS_TRAIT_DESCRIPTION,
                           _SYS_TRAIT_CATEGORY,
                           _SYS_TRAIT_TYPE]

        traits_meta.append({key: trait[key] for key in trait_meta_keys})

    heatmap_row = [{FIELD_HEATMAP_KBASE_ID: data_id,
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
                                                left_on=loader_common_names.MICROTRAIT_TRAIT_NAME,
                                                right_on=loader_common_names.SYS_TRAIT_ID,
                                                how='left')
        trait_counts_df.drop(columns=[loader_common_names.SYS_TRAIT_ID], inplace=True)

        genes_detected_table, _ = _get_r_list_element(microtrait_result, 'genes_detected_table')
        genes_detected_df = _r_table_to_df(genes_detected_table)
        detected_genes_score = dict(zip(genes_detected_df[_GENE_NAME_COL], genes_detected_df[_GENE_SCORE_COL]))

        trait_counts_df[loader_common_names.DETECTED_GENE_SCORE_COL] = trait_counts_df[
            loader_common_names.UNWRAPPED_GENE_COL].apply(
            lambda x: json.dumps({gene: detected_genes_score.get(gene) for gene in str(x).split(';') if
                                  gene in detected_genes_score}))
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
