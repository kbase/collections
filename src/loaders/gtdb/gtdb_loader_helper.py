import jsonlines

"""
this module contains helper functions used for gtdb loaders (e.g. gtdb_genome_stats_helper, gtdb_taxa_freq_loader, etc.)
"""


def convert_to_json(docs, outfile):
    """
    Writes list of dictionaries to an argparse File (e.g. argparse.FileType('w')) object in JSON Lines formate.

    Args:
        docs: list of dictionaries
        outfile: an argparse File (e.g. argparse.FileType('w')) object
    """

    with jsonlines.Writer(outfile) as writer:
        writer.write_all(docs)
