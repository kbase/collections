"""
PROTOTYPE - Download genome files from NCBI FTP server.

usage: ncbi_downloader.py [-h] --download_file_ext
                          DOWNLOAD_FILE_EXT
                          [DOWNLOAD_FILE_EXT ...] --release_ver
                          {207,202,95,89,86,83,80}
                          [--root_dir ROOT_DIR]
                          [--collection COLLECTION]
                          [--threads THREADS]
                          [--chuck_size CHUCK_SIZE] [--overwrite]

options:
  -h, --help            show this help message and exit

required named arguments:
  --download_file_ext DOWNLOAD_FILE_EXT [DOWNLOAD_FILE_EXT ...]
                        Download only files that match given
                        extensions.
  --release_ver {207,202,95,89,86,83,80}
                        GTDB release version

optional arguments:
  --root_dir ROOT_DIR   Root directory. (default: /global/cfs/cdi
                        rs/kbase/collections/sourcedata)
  --collection COLLECTION
                        Collection (default: GTDB)
  --threads THREADS     Number of threads. (default: half of
                        system cpu count)
  --chuck_size CHUCK_SIZE
                        Number of genomes per thread
  --overwrite           Overwrite existing files.


e.g.
python ncbi_downloader.py --download_file_ext genomic.gff.gz genomic.fna.gz --release_ver 207

NOTE:
NERSC file structure for GTDB:
/global/cfs/cdirs/kbase/collections/sourcedata/ -> GTDB -> [GTDB_release_version] -> [genome_id] -> genome files

e.g.
/global/cfs/cdirs/kbase/collections/sourcedata/GTDB -> r207 -> GB_GCA_000016605.1 -> genome files
                                                            -> GB_GCA_000200715.1 -> genome files
                                                       r202 -> GB_GCA_000016605.1 -> genome files
                                                            -> GB_GCA_000200715.1 -> genome files
                                                       r80  -> GB_GCA_000016605.1 -> genome files
                                                            -> GB_GCA_000200715.1 -> genome files


"""
import argparse
import itertools
import multiprocessing
import os
import time
from datetime import datetime
from urllib import request
from urllib.parse import urlparse

import ftputil

# Fraction amount of system cores can be utilized
# (i.e. 0.5 - program will use 50% of total processors,
#       0.1 - program will use 10% of total processors)
SYSTEM_UTILIZATION = 0.5

GTDB_RELEASE_VER = ['207', '202', '95', '89', '86', '83', '80']  # available GTDB release versions
COLLECTION = ['GTDB']  # supported collection


def _download_genome_file(download_dir: str, gene_id: str, target_file_ext: list[str], overwrite=False) -> None:
    # NCBI file structure: a delegated directory is used to store files for all versions of a genome
    # e.g. File structure for RS_GCF_000968435.2 (https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/968/435/)
    # genomes/all/GCF/000/968/435/ --> GCF_000968435.1_ASM96843v1/
    #                              --> GCF_000968435.2_ASM96843v2/
    ncbi_domain, user_name, password = 'ftp.ncbi.nlm.nih.gov', 'anonymous', 'anonymous@domain.com'

    success, attempts, max_attempts, gene_id = False, 0, 3, gene_id[3:]
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
                    if any([gene_file_name.endswith(ext) for ext in target_file_ext]):
                        result_file_path = os.path.join(download_dir, gene_file_name)
                        if overwrite or not os.path.exists(result_file_path):
                            host.download(gene_file_name, result_file_path)

            success = True
        except Exception as e:
            print(f'Error:\n{e}\nfrom attempt {attempts + 1}.\nTrying to rerun.')
            attempts += 1

    if not success:
        raise ValueError(f'Download Failed for {gene_id} after {max_attempts} attempts!')


def _form_gtdb_taxonomy_file_url(release_ver):
    # form GTDB taxonomy URL for specific GTDB version.
    # e.g. https://data.gtdb.ecogenomic.org/releases/release207/207.0/ar53_taxonomy_r207.tsv

    gtdb_server = 'https://data.gtdb.ecogenomic.org/releases/'
    file_dir_url = gtdb_server + f'release{release_ver}/' + f'{release_ver}.0/'

    file_urls = list()
    if release_ver in ['207', '202', '95', '89']:

        ar_ver = '53' if release_ver in ['207'] else '122'

        ar_taxonomy_url = file_dir_url + f'ar{ar_ver}_taxonomy_r{release_ver}.tsv'
        bac_taxonomy_url = file_dir_url + f'bac120_taxonomy_r{release_ver}.tsv'

        file_urls.extend([ar_taxonomy_url, bac_taxonomy_url])

    elif release_ver in ['86']:
        ar_taxonomy_url = file_dir_url + f'arc_taxonomy_r{release_ver}.tsv'
        bac_taxonomy_url = file_dir_url + f'bac_taxonomy_r{release_ver}.tsv'

        file_urls.extend([ar_taxonomy_url, bac_taxonomy_url])
    elif release_ver in ['83', '80']:  # no arc taxonomy file exists
        bac_taxonomy_url = file_dir_url + f'bac_taxonomy_r{release_ver}.tsv'

        file_urls.extend([bac_taxonomy_url])
    else:
        raise ValueError(f'Unsupported GTDB release version: {release_ver}')

    return file_urls


def _fetch_gtdb_genome_ids(release_ver, work_dir):
    # download GTDB taxonomy files and then parse genome_ids from the GTDB taxonomy files
    genome_ids = list()

    taxonomy_urls = _form_gtdb_taxonomy_file_url(release_ver)

    for taxonomy_url in taxonomy_urls:
        # download GTDB taxonomy file to work_dir
        url = urlparse(taxonomy_url)
        taxonomy_file = os.path.join(work_dir, os.path.basename(url.path))
        _ = request.urlretrieve(taxonomy_url, taxonomy_file)

        # parse genome id (first column of tsv file)
        with open(taxonomy_file, 'r') as f:
            genome_ids.extend([line.strip().split("\t")[0] for line in f])

    return genome_ids


def _fetch_genome_ids(collection, release_ver, work_dir):
    # retrieve collection genome ids

    if collection == 'GTDB':
        genome_ids = _fetch_gtdb_genome_ids(release_ver, work_dir)
    else:
        raise ValueError(f'Unexpected collection: {collection}')

    return genome_ids


def _make_work_dir(root_dir, collection, release_ver):
    # make working directory for a specific collection under root directory

    if collection == 'GTDB':
        work_dir = os.path.join(root_dir, collection, f'r{release_ver}')
    else:
        raise ValueError(f'Unexpected collection: {collection}')

    os.makedirs(work_dir, exist_ok=True)

    return work_dir


def download_genome_files(gene_ids: list[str], target_file_ext: list[str], result_dir='ncbi_genomes',
                          overwrite=False) -> list[str]:
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
            _download_genome_file(download_dir, gene_id, target_file_ext, overwrite=overwrite)
        except Exception as e:
            print(e)
            failed_ids.append(gene_id)

        counter += 1

    if failed_ids:
        print(f'Failed to download {failed_ids}')

    return failed_ids


def main():
    parser = argparse.ArgumentParser(
        description='PROTOTYPE - Download genome files from NCBI FTP server.')

    required = parser.add_argument_group('required named arguments')
    optional = parser.add_argument_group('optional arguments')

    # Required flag argument
    required.add_argument('--download_file_ext', required=True, type=str, nargs='+',
                          help='Download only files that match given extensions.')
    required.add_argument('--release_ver', required=True, type=str, choices=GTDB_RELEASE_VER,
                          help='GTDB release version')

    # Optional argument
    optional.add_argument('--root_dir', type=str, default='/global/cfs/cdirs/kbase/collections/sourcedata',
                          help='Root directory. (default: /global/cfs/cdirs/kbase/collections/sourcedata)')
    optional.add_argument('--collection', type=str, default='GTDB',
                          help='Collection (default: GTDB)')
    optional.add_argument('--threads', type=int,
                          help='Number of threads. (default: half of system cpu count)')
    optional.add_argument('--chuck_size', type=int,
                          help='Number of genomes per thread')
    optional.add_argument('--overwrite', action='store_true',
                          help='Overwrite existing files.')

    args = parser.parse_args()

    (download_file_ext,
     release_ver,
     root_dir,
     collection,
     threads,
     chuck_size,
     overwrite) = (args.download_file_ext, args.release_ver, args.root_dir, args.collection,
                   args.threads, args.chuck_size, args.overwrite)

    if collection not in COLLECTION:
        raise ValueError(f'Unexpected collection. Currently supported collections: {COLLECTION}')

    work_dir = _make_work_dir(root_dir, collection, release_ver)

    genome_ids = _fetch_genome_ids(collection, release_ver, work_dir)

    if not threads:
        threads = max(int(multiprocessing.cpu_count() * min(SYSTEM_UTILIZATION, 1)), 1)
    print(f"Start downloading genome files with {threads} threads")

    if not chuck_size:
        chuck_size = max(len(genome_ids) // (threads - 1), 1)  # distribute genome ids evenly across threads
    batch_input = [(genome_ids[i: i + chuck_size], download_file_ext, work_dir, overwrite) for i in
                   range(0, len(genome_ids), chuck_size)]
    pool = multiprocessing.Pool(processes=threads)
    batch_result = pool.starmap(download_genome_files, batch_input)

    failed_ids = list(itertools.chain.from_iterable(batch_result))
    if failed_ids:
        print(f'Failed to download {failed_ids}')
    else:
        print(f'Successfully downloaded {len(genome_ids)} genome files')


if __name__ == "__main__":
    main()
