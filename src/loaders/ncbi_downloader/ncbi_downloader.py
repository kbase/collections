"""
usage: ncbi_downloader.py [-h] --download_file_ext DOWNLOAD_FILE_EXT [DOWNLOAD_FILE_EXT ...] --release_ver {202,207,214,80,83,86,89,95}
                          [--kbase_collection {GTDB}] [--root_dir ROOT_DIR] [--source {NCBI}] [--threads THREADS] [--overwrite]
                          [--exclude_name_substring EXCLUDE_NAME_SUBSTRING [EXCLUDE_NAME_SUBSTRING ...]]

PROTOTYPE - Download genome files from NCBI FTP server.

options:
  -h, --help            show this help message and exit

required named arguments:
  --download_file_ext DOWNLOAD_FILE_EXT [DOWNLOAD_FILE_EXT ...]
                        Download only files that match given extensions
  --release_ver {202,207,214,80,83,86,89,95}
                        GTDB release version

optional arguments:
  --kbase_collection {GTDB}
                        Create a collection and link in data to that collection from the overall source data dir (default: GTDB)
  --root_dir ROOT_DIR   Root directory (default: /global/cfs/cdirs/kbase/collections)
  --source {NCBI}       Source of data (default: NCBI)
  --threads THREADS     Number of threads
  --overwrite           Overwrite existing files
  --exclude_name_substring EXCLUDE_NAME_SUBSTRING [EXCLUDE_NAME_SUBSTRING ...]
                        Files with a specific substring in their names that should be excluded from the download (default: [])

                        
e.g.
PYTHONPATH=. python src/loaders/ncbi_downloader/ncbi_downloader.py --download_file_ext genomic.fna.gz --release_ver 207 
                                                                   --exclude_name_substring cds_from rna_from ERR

NOTE:
NERSC file structure for NCBI:
/global/cfs/cdirs/kbase/collections/sourcedata/ -> NCBI -> ENV -> genome_id -> genome files

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/NCBI -> NONE -> GCA_000016605.1 -> genome files
                                                            -> GCA_000200715.1 -> genome files                  
                                                            -> GCF_000970165.1 -> genome files
                                                            -> GCF_000970185.1 -> genome files

The data will be linked to the collections source directory:
e.g. /global/cfs/cdirs/kbase/collections/collectionssource/ -> ENV -> kbase_collection -> release_ver -> genome_id -> genome files
"""
import argparse
import itertools
import math
import os
import sys
import time
from datetime import datetime
from multiprocessing import cpu_count, Pool
from urllib import request
from urllib.parse import urlparse

import ftputil
import requests
from bs4 import BeautifulSoup

from src.loaders.common import loader_common_names, loader_helper
from src.loaders.common.loader_helper import parse_genome_id

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5
SOURCE = ["NCBI"]  # supported source of data
COLLECTIONS = ["GTDB"]  # supported collections
GTDB_DOMAIN = "https://data.gtdb.ecogenomic.org/releases/"
ENV = "NONE"


def _parse_gtdb_release_vers() -> list[str]:
    # parse GTDB release versions from GTDB website (FTP port is closed)

    response = requests.get(GTDB_DOMAIN)
    soup = BeautifulSoup(response.text, 'html.parser')

    links = soup.find_all('a')

    # release links follow format like 'latest/', 'release202/', 'release83/', etc.
    release_ver = [link.get('href').split('release')[-1][:-1] for link in links if 'release' in link.get('href')]

    return release_ver


def _download_genome_file(
        download_dir: str, 
        gene_id: str, 
        target_file_ext: list[str],
        exclude_name_substring: list[str],
        overwrite: bool = False
) -> None:
    # NCBI file structure: a delegated directory is used to store files for all versions of a genome
    # e.g. File structure for RS_GCF_000968435.2 (https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/968/435/)
    # genomes/all/GCF/000/968/435/ --> GCF_000968435.1_ASM96843v1/
    #                              --> GCF_000968435.2_ASM96843v2/

    ncbi_domain, user_name, password = 'ftp.ncbi.nlm.nih.gov', 'anonymous', 'anonymous@domain.com'

    success, attempts, max_attempts = False, 0, 3
    while attempts < max_attempts and not success:
        try:
            time.sleep(5 * attempts)
            with ftputil.FTPHost(ncbi_domain, user_name, password) as host:

                gene_dir = '/genomes/all/{}/{}/{}/{}/'.format(
                    gene_id[0:3], gene_id[4:7], gene_id[7:10], gene_id[10:13])

                host.chdir(gene_dir)
                dir_list = host.listdir(host.curdir)
                gene_dir_name = [i for i in dir_list if i.startswith(gene_id)][0]
                host.chdir(gene_dir_name)
                gene_file_list = host.listdir(host.curdir)

                for gene_file_name in gene_file_list:
                    # file has target extensions but doesn't contain exclude name substring
                    if any([gene_file_name.endswith(ext) for ext in target_file_ext]) and all(
                            [substring not in gene_file_name for substring in exclude_name_substring]):
                        result_file_path = os.path.join(download_dir, gene_file_name)
                        if overwrite or not os.path.exists(result_file_path):
                            host.download(gene_file_name, result_file_path)

            success = True
        except Exception as e:
            print(f'Error:\n{e}\nfrom attempt {attempts + 1}.\nTrying to rerun.')
            attempts += 1

    if not success:
        raise ValueError(f'Download Failed for {gene_id} after {max_attempts} attempts!')


def _form_gtdb_taxonomy_file_url(release_ver: str) -> list[str]:
    # form GTDB taxonomy URL for specific GTDB version.
    # e.g. https://data.gtdb.ecogenomic.org/releases/release207/207.0/ar53_taxonomy_r207.tsv

    file_dir_url = GTDB_DOMAIN + f'release{release_ver}/' + f'{release_ver}.0/'  # TODO: add dot support, relase version 86 has '86.1' and '86.2'

    file_urls = list()

    # parse taxonomy file URL for genome ids
    response = requests.get(file_dir_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    for link in soup.find_all('a'):
        href = link.get('href')

        if href.endswith('tsv') and '_taxonomy_' in href:
            file_urls.append(file_dir_url + href)

    if not file_urls or len(file_urls) >= 3:
        raise ValueError(
            f'Parsed unexpected taxonomy files {file_urls}. '
            f'Please check parsing logic in _form_gtdb_taxonomy_file_url.')

    return file_urls


def _fetch_gtdb_genome_ids(release_ver: str, work_dir: str) -> list[str]:
    # download GTDB taxonomy files and then parse genome_ids from the GTDB taxonomy files
    genome_ids = list()

    taxonomy_urls = _form_gtdb_taxonomy_file_url(release_ver)

    for taxonomy_url in taxonomy_urls:
        # download GTDB taxonomy file to work_dir
        url = urlparse(taxonomy_url)
        taxonomy_file = os.path.join(work_dir, os.path.basename(url.path))
        request.urlretrieve(taxonomy_url, taxonomy_file)

        # parse genome id from first column of GTDB taxonomy tsv file
        with open(taxonomy_file, 'r') as f:
            genome_ids.extend([parse_genome_id(line.strip().split("\t")[0]) for line in f])

    return genome_ids


def _fetch_genome_ids(kbase_collection: str, release_ver: str, work_dir: str) -> list[str]:
    # retrieve genome ids
    func_name = f"_fetch_{kbase_collection.lower()}_genome_ids"

    try:
        fetch_coll_genome_ids = getattr(sys.modules[__name__], func_name)
    except AttributeError as e:
        raise ValueError(f"Please implement fetching method for: {func_name}") from e
    
    genome_ids = fetch_coll_genome_ids(release_ver, work_dir)

    return genome_ids


def _make_work_dir(root_dir: str, source_data_dir: str, source: str, env: str) -> str:
    # make working directory for a specific collection under root directory
    work_dir = os.path.join(root_dir, source_data_dir, source, env)
    os.makedirs(work_dir, exist_ok=True)
    return work_dir


def _make_collection_source_dir(
        root_dir: str,
        collection_source_dir: str,
        collection: str,
        release_ver: str,
        env: str
) -> str:
    """
    Helper function that creates a collection & source_version and link in data
    to that colleciton from the overall source data dir.
    """
    csd = os.path.join(root_dir, collection_source_dir, env, collection, release_ver)
    os.makedirs(csd, exist_ok=True)
    return csd


def _process_genome_ids(
        work_dir: str,
        genome_ids_unprocssed: list[str], 
        target_file_ext: list[str],
        exclude_name_substring: list[str],
        overwrite: bool = False
)-> list[str]:
    """
    Helper function that processes genome ids to avoid redownloading files.
    """
    if overwrite:
        return genome_ids_unprocssed
    
    genome_ids = list()
    for genome_id in genome_ids_unprocssed:
        data_dir = os.path.join(work_dir, genome_id)
        if not os.path.exists(data_dir):
            genome_ids.append(genome_id)
            continue
        
        exist = False
        for file_name in os.listdir(data_dir):
            if any([file_name.endswith(ext) for ext in target_file_ext]) and all(
                    [substring not in file_name for substring in exclude_name_substring]):
                exist = True
                break
        
        if not exist:
            genome_ids.append(genome_id)

    return genome_ids


def download_genome_files(
        gene_ids: list[str],
        target_file_ext: list[str],
        exclude_name_substring: list[str],
        result_dir: str,
        overwrite: bool = False
) -> list[str]:
    """
    Download genome files from NCBI FTP server

    gene_ids: genome ids that parsed from GTDB metadata file (e.g. GB_GCA_000016605.1, RS_GCF_000968435.2).
    target_file_ext: download only files that match given extensions.
    result_dir: result directory for downloaded files. Files for a specific gene ID are stored in a folder with the gene ID as the name.
    """

    print(f'start downloading {len(gene_ids)} genome files')

    failed_ids = list()

    os.makedirs(result_dir, exist_ok=True)

    counter = 1
    for gene_id in gene_ids:
        if counter % 5000 == 0:
            print(f"{round(counter / len(gene_ids), 4) * 100}% finished at {datetime.now()}")

        download_dir = os.path.join(result_dir, gene_id)
        os.makedirs(download_dir, exist_ok=True)

        try:
            _download_genome_file(download_dir, gene_id, target_file_ext, exclude_name_substring, overwrite=overwrite)
        except Exception as e:
            print(e)
            failed_ids.append(gene_id)

        counter += 1

    if failed_ids:
        print(f'Failed to download {failed_ids}')

    return failed_ids


def create_softlink_between_csd_and_work_dir(
        csd: str,
        work_dir: str,
        genome_ids_unprocssed: list[str],
        failed_ids: list[str] = []
) -> None:
    genome_ids = set(genome_ids_unprocssed) - set(failed_ids)
    for genome_id in genome_ids:
        genome_dir = os.path.join(work_dir, genome_id)
        csd_genome_dir = os.path.join(csd, genome_id)
        loader_helper.create_softlink(csd_genome_dir, genome_dir)

    print(f"Genome files in {csd} \nnow link to {work_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Download genome files from NCBI FTP server.',
        formatter_class=loader_helper.ExplicitDefaultsHelpFormatter,
    )

    gtdb_release_vers = _parse_gtdb_release_vers()  # available GTDB release versions

    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required flag argument
    required.add_argument('--download_file_ext', required=True, type=str, nargs='+',
                          help='Download only files that match given extensions')
    required.add_argument('--release_ver', required=True, type=str, choices=gtdb_release_vers,
                          help='GTDB release version')

    # Optional argument
    optional.add_argument(f"--{loader_common_names.KBASE_COLLECTION_ARG_NAME}", type=str, default="GTDB", choices=COLLECTIONS,
                          help="Create a collection and link in data to that collection from the overall source data dir")
    optional.add_argument('--root_dir', type=str, default=loader_common_names.ROOT_DIR,
                          help='Root directory')
    optional.add_argument('--source', type=str, default='NCBI', choices=SOURCE, 
                          help='Source of data')
    optional.add_argument('--threads', type=int,
                          help='Number of threads')
    optional.add_argument('--overwrite', action='store_true',
                          help='Overwrite existing files')
    optional.add_argument('--exclude_name_substring', type=str, nargs='+', default=[],
                          help='Files with a specific substring in their names that should be excluded from the download')

    args = parser.parse_args()
    
    download_file_ext = args.download_file_ext
    release_ver = args.release_ver
    root_dir = args.root_dir
    source = args.source
    threads = args.threads
    overwrite = args.overwrite
    exclude_name_substring = args.exclude_name_substring
    kbase_collection = getattr(args, loader_common_names.KBASE_COLLECTION_ARG_NAME)

    if threads and (threads < 1 or threads > cpu_count()):
        parser.error(f"minimum thread is 1 and maximum thread is {cpu_count()}")

    work_dir = _make_work_dir(root_dir, loader_common_names.SOURCE_DATA_DIR, source, ENV)
    csd = _make_collection_source_dir(root_dir, loader_common_names.COLLECTION_SOURCE_DIR, kbase_collection, release_ver, ENV)
    genome_ids_unprocssed = _fetch_genome_ids(kbase_collection, release_ver, work_dir)
    genome_ids = _process_genome_ids(work_dir, genome_ids_unprocssed, download_file_ext, exclude_name_substring, overwrite)
    if not genome_ids:
        print(f"All {len(genome_ids_unprocssed)} genomes files haven already existed in {work_dir}")
        create_softlink_between_csd_and_work_dir(csd, work_dir, genome_ids_unprocssed)
        return

    if not threads:
        threads = max(int(cpu_count() * min(SYSTEM_UTILIZATION, 1)), 1)
    threads = max(1, threads)
    
    print(f"Originally planned to download {len(genome_ids_unprocssed)} genome files\n"
          f"Detected {len(genome_ids_unprocssed) - len(genome_ids)} genome files already existed\n"
          f"Start downloading {len(genome_ids)} genome files with {threads} threads\n")

    chunk_size = math.ceil(len(genome_ids) / threads)  # distribute genome ids evenly across threads
    batch_input = [(genome_ids[i: i + chunk_size], download_file_ext, exclude_name_substring, work_dir,
                    overwrite) for i in range(0, len(genome_ids), chunk_size)]
    pool = Pool(processes=threads)
    batch_result = pool.starmap(download_genome_files, batch_input)

    failed_ids = list(itertools.chain.from_iterable(batch_result))
    if failed_ids:
        print(f'\nFailed to download {failed_ids}')
    else:
        print(f'\nSuccessfully downloaded {len(genome_ids)} genome files')
    
    create_softlink_between_csd_and_work_dir(csd, work_dir, genome_ids_unprocssed, failed_ids)
    

if __name__ == "__main__":
    main()
