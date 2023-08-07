"""
usage: gtdb.py [-h] --release_ver {202,207,214,80,83,86,89,95} [--root_dir ROOT_DIR]
               [--download_file_ext DOWNLOAD_FILE_EXT [DOWNLOAD_FILE_EXT ...]] [--threads THREADS] [--overwrite]
               [--exclude_name_substring EXCLUDE_NAME_SUBSTRING [EXCLUDE_NAME_SUBSTRING ...]]

PROTOTYPE - Download genome files from NCBI FTP server.

options:
  -h, --help            show this help message and exit

required named arguments:
  --release_ver {202,207,214,80,83,86,89,95}
                        GTDB release version that dynamically parsed from the GTDB website

optional arguments:
  --root_dir ROOT_DIR   Root directory for the collections project. (default: /global/cfs/cdirs/kbase/collections)
  --download_file_ext DOWNLOAD_FILE_EXT [DOWNLOAD_FILE_EXT ...]
                        Download only files that match given extensions (default: ['genomic.fna.gz'])
  --threads THREADS     Number of threads
  --overwrite           Overwrite existing files
  --exclude_name_substring EXCLUDE_NAME_SUBSTRING [EXCLUDE_NAME_SUBSTRING ...]
                        Files with a specific substring in their names that should be excluded from the download (default: ['cds_from',
                        'rna_from', 'ERR'])

e.g.
PYTHONPATH=. python src/loaders/ncbi_downloader/gtdb.py --release_ver 207 --download_file_ext genomic.fna.gz

NOTE:
NERSC file structure for NCBI:
/global/cfs/cdirs/kbase/collections/sourcedata/ -> NCBI -> ENV -> genome_id -> genome files

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/NCBI -> NONE -> GCA_000016605.1 -> genome files
                                                            -> GCA_000200715.1 -> genome files                  
                                                            -> GCF_000970165.1 -> genome files
                                                            -> GCF_000970185.1 -> genome files

The data will be linked to the collections source directory:
e.g. /global/cfs/cdirs/kbase/collections/collectionssource/ -> ENV -> kbase_collection -> source_ver -> genome_id -> genome files
"""
import argparse
import itertools
import os
from multiprocessing import cpu_count
from typing import Tuple
from urllib import request
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from src.loaders.common import loader_common_names, loader_helper
from src.loaders.common.loader_helper import parse_genome_id
from src.loaders.ncbi_downloader import ncbi_downloader_helper

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5
DOWNLOAD_FILE_EXT = ["genomic.fna.gz"]  # download only files that match given extensions
KBASE_COLLECTION = "GTDB"  # GTDB is the only collection supported by this script
GTDB_DOMAIN = "https://data.gtdb.ecogenomic.org/releases/"


def _parse_gtdb_release_vers() -> list[str]:
    # parse GTDB release versions from GTDB website (FTP port is closed)

    response = requests.get(GTDB_DOMAIN)
    soup = BeautifulSoup(response.text, "html.parser")

    links = soup.find_all("a")

    # release links follow format like 'latest/', 'release202/', 'release83/', etc.
    release_ver = [
        link.get("href").split("release")[-1][:-1]
        for link in links
        if "release" in link.get("href")
    ]

    return release_ver


def _form_gtdb_taxonomy_file_url(release_ver: str) -> list[str]:
    # form GTDB taxonomy URL for specific GTDB version.
    # e.g. https://data.gtdb.ecogenomic.org/releases/release207/207.0/ar53_taxonomy_r207.tsv

    file_dir_url = (
        GTDB_DOMAIN + f"release{release_ver}/" + f"{release_ver}.0/"
    )  # TODO: add dot support, release version 86 has '86.1' and '86.2'

    file_urls = list()

    # parse taxonomy file URL for genome ids
    response = requests.get(file_dir_url)
    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href")

        if href.endswith("tsv") and "_taxonomy_" in href:
            file_urls.append(file_dir_url + href)

    if not file_urls or len(file_urls) >= 3:
        raise ValueError(
            f"Parsed unexpected taxonomy files {file_urls}. "
            f"Please check parsing logic in _form_gtdb_taxonomy_file_url."
        )

    return file_urls


def _fetch_gtdb_genome_ids(release_ver: str, work_dir: str) -> Tuple[list[str], list[str]]:
    # download GTDB taxonomy files and then parse genome_ids from the GTDB taxonomy files
    genome_ids = list()

    taxonomy_urls = _form_gtdb_taxonomy_file_url(release_ver)

    for taxonomy_url in taxonomy_urls:
        # download GTDB taxonomy file to work_dir
        url = urlparse(taxonomy_url)
        taxonomy_file = os.path.join(work_dir, os.path.basename(url.path))
        request.urlretrieve(taxonomy_url, taxonomy_file)

        # parse genome id from first column of GTDB taxonomy tsv file
        with open(taxonomy_file, "r") as f:
            genome_ids.extend(
                [parse_genome_id(line.strip().split("\t")[0]) for line in f]
            )

    return genome_ids, taxonomy_urls


def _get_parser():

    parser = argparse.ArgumentParser(
        description="PROTOTYPE - Download genome files from NCBI FTP server.",
        formatter_class=loader_helper.ExplicitDefaultsHelpFormatter,
    )

    gtdb_release_vers = _parse_gtdb_release_vers()  # available GTDB release versions

    required = parser.add_argument_group("required named arguments")
    optional = parser.add_argument_group("optional arguments")

    # Required flag argument
    required.add_argument(
        "--release_ver",
        required=True,
        type=str,
        choices=gtdb_release_vers,
        help="GTDB release version",
    )

    # Optional argument
    optional.add_argument(
        f"--{loader_common_names.ROOT_DIR_ARG_NAME}",
        type=str,
        default=loader_common_names.ROOT_DIR,
        help=loader_common_names.ROOT_DIR_DESCR
    )
    optional.add_argument(
        "--download_file_ext",
        type=str,
        nargs="+",
        default=DOWNLOAD_FILE_EXT,
        help="Download only files that match given extensions",
    )
    optional.add_argument("--threads", type=int, help="Number of threads")
    optional.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing files"
    )
    optional.add_argument(
        "--exclude_name_substring",
        type=str,
        nargs="+",
        default=loader_common_names.STANDARD_FILE_EXCLUDE_SUBSTRINGS,
        help="Files with a specific substring in their names that should be excluded from the download",
    )
    return parser


def main():
    parser = _get_parser()
    args = parser.parse_args()

    release_ver = args.release_ver
    threads = args.threads
    overwrite = args.overwrite
    download_file_ext = args.download_file_ext
    exclude_name_substring = args.exclude_name_substring
    root_dir = getattr(args, loader_common_names.ROOT_DIR_ARG_NAME)

    if threads and (threads < 1 or threads > cpu_count()):
        parser.error(f"minimum thread is 1 and maximum thread is {cpu_count()}")

    work_dir = ncbi_downloader_helper.get_work_dir(root_dir)
    collection_source_dir = loader_helper.make_collection_source_dir(
        root_dir,
        loader_common_names.DEFAULT_ENV,
        KBASE_COLLECTION,
        release_ver,
    )
    genome_ids_unprocessed, taxonomy_urls = _fetch_gtdb_genome_ids(release_ver, work_dir)
    taxonomy_files = [os.path.basename(url) for url in taxonomy_urls]
    genome_ids = ncbi_downloader_helper.remove_ids_with_existing_data(
        root_dir,
        genome_ids_unprocessed,
        download_file_ext,
        exclude_name_substring,
        overwrite,
    )
    if not genome_ids:
        print(f"All {len(genome_ids_unprocessed)} genomes files already exist in {work_dir}")
        loader_helper.create_softlinks_in_collection_source_dir(
            collection_source_dir,
            work_dir,
            genome_ids_unprocessed,
            taxonomy_files,
        )
        return

    print(f"Originally planned to download {len(genome_ids_unprocessed)} genome files")
    print(
        f"Will overwrite existing genome files"
        if overwrite
        else f"Detected {len(genome_ids_unprocessed) - len(genome_ids)} genome files already exist"
    )

    batch_result = ncbi_downloader_helper.download_genome_files_in_parallel(
        root_dir,
        genome_ids,
        download_file_ext,
        exclude_name_substring,
        SYSTEM_UTILIZATION,
        threads,
        overwrite,
    )

    failed_ids = list(itertools.chain.from_iterable(batch_result))
    if failed_ids:
        print(f"\nFailed to download {failed_ids}")
    else:
        print(f"\nSuccessfully downloaded {len(genome_ids)} genome files")

    genome_ids_clean = set(genome_ids) - set(failed_ids)
    loader_helper.create_softlinks_in_collection_source_dir(
        collection_source_dir,
        work_dir,
        list(genome_ids_clean),
        taxonomy_files,
    )


if __name__ == "__main__":
    main()
