"""
Runs microtrait on a set of assemblies.
"""
import json
import os
from pathlib import Path

import pandas as pd
from rpy2 import robjects

from src.loaders.common import loader_common_names
from src.loaders.compute_tools.tool_common import ToolRunner, unpack_gz_file

# the name of the component used for extracting traits from microtrait's 'extract.traits' result
TRAIT_COUNTS_ATGRANULARITY = 'trait_counts_atgranularity3'
_GENE_NAME_COL = 'hmm_name'  # column name from the genes_detected_table file that contains the gene name
_GENE_SCORE_COL = 'gene_score'  # column name from the genes_detected_table file that contains the gene score


def _get_r_list_element(r_list, element_name):
    # retrieve the element from the R list
    pos = r_list.names.index(element_name)
    return r_list[pos]


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


def _run_microtrait(genome_id: str, fna_file: Path, genome_dir: Path, debug: bool):
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

    microtrait_result = _get_r_list_element(r_result, 'microtrait_result')

    try:
        trait_counts = _get_r_list_element(microtrait_result, TRAIT_COUNTS_ATGRANULARITY)
    except Exception as e:
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

        genes_detected_table = _get_r_list_element(microtrait_result, 'genes_detected_table')
        genes_detected_df = _r_table_to_df(genes_detected_table)
        detected_genes_score = dict(zip(genes_detected_df[_GENE_NAME_COL], genes_detected_df[_GENE_SCORE_COL]))

        trait_counts_df[loader_common_names.DETECTED_GENE_SCORE_COL] = trait_counts_df[
            loader_common_names.UNWRAPPED_GENE_COL].apply(
            lambda x: json.dumps({gene: detected_genes_score.get(gene) for gene in str(x).split(';') if
                                  gene in detected_genes_score}))
    else:
        raise ValueError('Please set environment variable MT_TRAIT_UNWRAPPED_FILE')

    trait_counts_df.to_csv(os.path.join(genome_dir, loader_common_names.TRAIT_COUNTS_FILE), index=False)


def main():
    runner = ToolRunner("microtrait")
    runner.parallel_single_execution(_run_microtrait, unzip=True)


if __name__ == "__main__":
    main()
