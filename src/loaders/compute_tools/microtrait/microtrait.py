"""
Runs microtrait on a set of assemblies.
"""

import pandas as pd
import os
from rpy2 import robjects
from src.loaders.compute_tools.tool_common import ToolRunner, unpack_gz_file
from pathlib import Path


def _get_r_list_element(r_list, element_name):
    # retrieve the element from the R list
    pos = r_list.names.index(element_name)
    return r_list[pos]


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

    remove_gz_file = False
    if fna_file.suffix == '.gz':
        remove_gz_file = True
        fna_file = unpack_gz_file(fna_file)
    r_result = r_func(str(fna_file), str(genome_dir))

    # retrieve genes_detected_table from microtrait_result and save it as a csv file
    # genes_detected_table includes the following columns:
    # gene_name, gene_len, hmm_name, gene_score, gene_from, gene_to, cov_gene, hmm_from,
    # hmm_to, cov_domain
    microtrait_result = _get_r_list_element(r_result, 'microtrait_result')
    genes_detected_table = _get_r_list_element(microtrait_result, 'genes_detected_table')
    data = dict()
    for idx, name in enumerate(genes_detected_table.names):
        data.update({name: list(genes_detected_table[idx])})
    genes_detected_df = pd.DataFrame(data=data)
    genes_detected_df.to_csv(genome_dir / 'genes_detected_table.csv', index=False)

    # remove the unpacked assembly file to save space
    if remove_gz_file:
        print(f'removing {fna_file}')
        os.remove(fna_file)


def main():
    runner = ToolRunner("microtrait")
    runner.run_single(_run_microtrait)


if __name__ == "__main__":
    main()
