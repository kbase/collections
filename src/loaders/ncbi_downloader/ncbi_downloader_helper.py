import os
import time
from datetime import datetime

import ftputil


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