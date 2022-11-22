"""
PROTOTYPE - Download genome files from NCBI FTP server.

usage: ncbi_downloader.py [-h] --download_file_ext
                          DOWNLOAD_FILE_EXT [DOWNLOAD_FILE_EXT ...]
                          --genome_id_files GENOME_ID_FILES
                          [GENOME_ID_FILES ...] --load_ver LOAD_VER
                          [--genome_id_col GENOME_ID_COL]
                          [--kbase_collection KBASE_COLLECTION]
                          [--root_dir ROOT_DIR] [--threads THREADS]
                          [--overwrite]

required flag argument:
  --download_file_ext DOWNLOAD_FILE_EXT [DOWNLOAD_FILE_EXT ...]
                        Download only files that match given
                        extensions.
  --genome_id_files GENOME_ID_FILES [GENOME_ID_FILES ...]
                        Files used to parse genome ids. (e.g.
                        ar53_metadata_r207.tsv)
  --load_ver LOAD_VER   KBase load version. (e.g. r207.kbase.1)

optional arguments:
  -h, --help            show this help message and exit
  --genome_id_col GENOME_ID_COL
                        Column from genome_id_files that stores
                        genome ids. (default: accession)
  --kbase_collection KBASE_COLLECTION
                        KBase collection identifier name. (default:
                        GTDB)
  --root_dir ROOT_DIR   Root directory. (default: /global/cfs/cdirs
                        /kbase/collections/genome_attributes)
  --threads THREADS     Number of threads. (default: half of system
                        cpu count)
  --overwrite           Overwrite existing files.

e.g.
python src/loaders/ncbi_downloader/ncbi_downloader.py --download_file_ext genomic.gff.gz genomic.fna.gz --genome_id_files ar53_metadata_r207.tsv bac120_metadata_r207.tsv --load_ver r207.kbase.2

NOTE:
NERSC file structure:
/global/cfs/cdirs/kbase/collections/genome_attributes -> [kbase_collection] -> [load_ver] -> [genome_id] -> genome files

e.g.
/global/cfs/cdirs/kbase/collections/genome_attributes -> GTDB -> r207.kbase.1 -> GB_GCA_000016605.1 -> genome files
                                                                              -> GB_GCA_000200715.1 -> genome files
                                                              -> r207.kbase.2 -> GB_GCA_000016605.1 -> genome files
                                                                              -> GB_GCA_000200715.1 -> genome files
                                                              -> r202.kbase.1 -> GB_GCA_000016605.1 -> genome files
                                                                              -> GB_GCA_000200715.1 -> genome files


"""
import argparse
import itertools
import multiprocessing
import os
import time
from datetime import datetime
from typing import List

import ftputil
import pandas as pd


def _download_genome_file(download_dir: str, gene_id: str, target_file_ext: List[str], overwrite=False) -> None:
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
            print(e)
            attempts += 1

    if not success:
        raise ValueError(f'Download Failed for {gene_id} after {max_attempts} attempts!')


def download_genome_files(gene_ids: List[str], target_file_ext: List[str], result_dir='ncbi_genomes',
                          overwrite=False) -> List[str]:
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

    # Required flag argument
    parser.add_argument('--download_file_ext', required=True, type=str, nargs='+',
                        help='Download only files that match given extensions.')
    parser.add_argument('--genome_id_files', required=True, type=str, nargs='+',
                        help='Files used to parse genome ids. (e.g. ar53_metadata_r207.tsv)')
    parser.add_argument('--load_ver', required=True, type=str,
                        help='KBase load version. (e.g. r207.kbase.1)')

    # Optional argument
    parser.add_argument('--genome_id_col', type=str, default='accession',
                        help='Column from genome_id_files that stores genome ids. (default: accession)')
    parser.add_argument('--kbase_collection', type=str, default='GTDB',
                        help='KBase collection identifier name. (default: GTDB)')
    parser.add_argument('--root_dir', type=str, default='/global/cfs/cdirs/kbase/collections/genome_attributes',
                        help='Root directory. (default: /global/cfs/cdirs/kbase/collections/genome_attributes)')
    parser.add_argument('--threads', type=int,
                        help='Number of threads. (default: half of system cpu count)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing files.')

    args = parser.parse_args()

    (download_file_ext,
     genome_id_files,
     genome_id_col,
     load_ver,
     kbase_collection,
     root_dir,
     threads,
     overwrite) = (args.download_file_ext, args.genome_id_files, args.genome_id_col, args.load_ver,
                   args.kbase_collection, args.root_dir, args.threads, args.overwrite)

    work_dir = os.path.join(root_dir, kbase_collection,
                            load_ver)  # working directory for genome downloads e.g. root_dir/GTDB/r207.kbase.1
    os.makedirs(work_dir, exist_ok=True)

    gene_ids = list()
    for gene_id_file in genome_id_files:
        gene_file_path = os.path.join(work_dir, gene_id_file)
        df = pd.read_csv(gene_file_path, sep='\t')
        gene_ids += df[genome_id_col].to_list()

    if not threads:
        threads = multiprocessing.cpu_count() // 2  # utilize half of system cups
    print(f"Start download genome files with {threads} threads")

    chuck_size = len(gene_ids) // (threads - 1)
    batch_input = [(gene_ids[i: i + chuck_size], download_file_ext, work_dir, overwrite) for i in
                   range(0, len(gene_ids), chuck_size)]  # distribute genome ids evenly across threads
    pool = multiprocessing.Pool(processes=threads)
    batch_result = pool.starmap(download_genome_files, batch_input)

    failed_ids = list(itertools.chain.from_iterable(batch_result))
    if failed_ids:
        print(f'Failed to download {failed_ids}')


if __name__ == "__main__":
    main()
