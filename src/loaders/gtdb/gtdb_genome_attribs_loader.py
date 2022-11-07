import argparse

import pandas as pd

import gtdb_genome_attribs_helper as helper
from gtdb_loader_helper import convert_to_json

from src.common.hash import md5_string
from src.common.storage.collection_and_field_names import (
    FLD_ARANGO_KEY,
    FLD_COLLECTION_ID,
    FLD_LOAD_VERSION,
)

"""
PROTOTYPE

Prepare GTDB genome attributes data in JSON format for arango import.

This script will first parse genome features from the GTDB metadata files. Those features should be put into
EXIST_FEATURES. 

It will then try to compute other(non-existing) features. The name of non-existing features should be put into 
NON_EXIST_FEATURES. Each non-existing features should have a corresponding function defined in gtdb_genome_attribs_helper.
NOTE: the function name should be exactly the same as the non-existing feature name.

Any features that are used for computing the non-existing features should be added to HELPER_FEATURES. They will be
eventually dropped from the result dataframe.


usage: gtdb_genome_attribs_loader.py [-h]
                                   --load_version
                                   LOAD_VERSION
                                   [--kbase_collection KBASE_COLLECTION]
                                   [-o OUTPUT]
                                   load_files
                                   [load_files ...]


e.g. gtdb_genome_attribs_loader.py bac120_metadata_r207.tsv ar53_metadata_r207.tsv --load_version 207
     gtdb_genome_attribs_loader.py bac120_metadata_r207.tsv ar53_metadata_r207.tsv --load_version 207 --kbase_collection gtdb
     gtdb_genome_attribs_loader.py bac120_metadata_r207.tsv ar53_metadata_r207.tsv --load_version 207 --kbase_collection gtdb --output  gtdb_genome_attribs.json
"""

EXIST_FEATURES = helper.EXIST_FEATURES
NON_EXIST_FEATURES = {'high_checkm_marker_count'}  # genome statistics to be computed
HELPER_FEATURES = {'checkm_marker_count'}  # additional features needed to compute statistics

# change header to specific context (default mapper: capitalize first character)
HEADER_MAPPER = {'accession': 'Genome Name'}


def _compute_attribs(df, computations):
    """
    Applies computation method on dataframe to create new columns storing the result of the computation

    NOTE: All computation method should be defined in gtdb_load_helper module
          The computation method should return a series which will be then appended to the dataframe

    Args:
        df:  A data frame object
        computations: List of computation method. (NOTE: Methods should be defined in the gtdb_load_helper module)
    """
    for computation in computations:
        try:
            comp_ops = getattr(helper, computation)
        except KeyError as e:
            raise ValueError(f'Please implement method [{computation}]') from e
        result = comp_ops(df)
        df[result.name] = result

    return df


def _parse_from_metadata_file(load_files, exist_features, additional_features):
    """
    Fetches certain columns (combination of exist_features and additional_features) from GTDB metadata file
    and saves result as a pandas data from
    """

    frames = [pd.read_csv(load_file, sep='\t', header=0, usecols=exist_features.union(additional_features)) for
              load_file in load_files]
    df = pd.concat(frames, ignore_index=True)

    return df


def _rename_col(df, header_mapper):
    """
    Formats data frame's column for easier readability

    Creates a default mapper that capitalizes headers (e.g. {'aa_bb_cc': 'Aa Bb Cc'} given 'aa_bb_cc' is one of
    dataframe's column) and then updates the mapper with user's mapper. After than changes dataframe's header in place.

     Args:
        df:  A data frame object
        header_mapper: A user input mapper to map specific col to user desired name

     Returns:
         None (updates dataframe in place)
    """

    # default mapper (e.g. {'aa_bb_cc': 'Aa Bb Cc'})
    # Note: all columns are following 'a_b_c' format in the original GTDB metadata file
    mapper = {col: ' '.join(list(map(lambda x: x.capitalize(), col.split('_')))) for col in df.columns}
    mapper.update(header_mapper)
    df.rename(columns=mapper, errors="raise", inplace=True)


def _row_to_doc(row, kbase_collection, load_version):
    """
    Transforms row (from a dataframe) into ArangoDB collection document
    """
    doc = {
        FLD_ARANGO_KEY: md5_string(f"{kbase_collection}_{load_version}_{row.name}"),
        FLD_COLLECTION_ID: kbase_collection,
        FLD_LOAD_VERSION: load_version,
    }
    doc.update(row.to_dict())

    return doc


def _df_to_docs(df, kbase_collection, load_version, header_mapper):
    _rename_col(df, header_mapper)
    docs = df.apply(_row_to_doc, args=(kbase_collection, load_version), axis=1).to_list()

    return docs


def main():
    if not all([header in EXIST_FEATURES.union(NON_EXIST_FEATURES) for header in HEADER_MAPPER.keys()]):
        raise ValueError('Please make sure HEADER_MAPPER keys are all included in the FEATURES')

    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Prepare GTDB genome statistics data in JSON format for arango import.')

    # Required positional argument
    parser.add_argument('load_files', type=argparse.FileType('r'), nargs='+',
                        help='GTDB genome metadata files')

    # Required flag argument
    parser.add_argument(
        '--load_version', required=True, type=str, nargs=1, help='KBase load version')

    # Optional argument
    parser.add_argument('--kbase_collection', type=str, default='gtdb',
                        help='kbase collection identifier name (default: gtdb)')
    parser.add_argument("-o", "--output", type=argparse.FileType('w'), default='gtdb_genome_attribs.json',
                        help="output JSON file path (default: gtdb_genome_attribs.json")

    args = parser.parse_args()
    load_files, load_version, kbase_collection = args.load_files, args.load_version[0], args.kbase_collection

    print('start parsing input files')
    df = _parse_from_metadata_file(load_files, EXIST_FEATURES, HELPER_FEATURES)
    df = _compute_attribs(df, NON_EXIST_FEATURES)
    df.drop(columns=HELPER_FEATURES - EXIST_FEATURES - NON_EXIST_FEATURES,
            inplace=True)  # drop features added for computation
    docs = _df_to_docs(df, kbase_collection, load_version, HEADER_MAPPER)

    with args.output as genome_attribs_json:
        convert_to_json(docs, genome_attribs_json)


if __name__ == "__main__":
    main()
