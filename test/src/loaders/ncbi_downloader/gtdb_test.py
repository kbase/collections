import os
import shutil
import uuid
from pathlib import Path

import pytest

from src.loaders.ncbi_downloader import gtdb, ncbi_downloader_helper
from src.loaders.ncbi_downloader.gtdb import GTDB_DOMAIN


@pytest.fixture(scope="module")
def setup_and_teardown():
    print('starting NCBI downloader tests')
    tmp_dir = 'result_{}'.format(str(uuid.uuid4()))
    os.makedirs(tmp_dir, exist_ok=True)

    caller_filename_full = Path(__file__).resolve()
    caller_file_dir = os.path.dirname(caller_filename_full)

    project_dir = Path(caller_file_dir).resolve().parents[3]
    script_file = f'{project_dir}/src/loaders/ncbi_downloader/gtdb.py'

    yield tmp_dir, script_file

    shutil.rmtree(tmp_dir, ignore_errors=True)


def test_make_work_dir(setup_and_teardown):
    tmp_dir, script_file = setup_and_teardown

    source, env = 'NCBI', 'NONE'
    work_dir = ncbi_downloader_helper.get_work_dir(tmp_dir)

    path = Path(work_dir).resolve()

    assert path.name == f'{env}'
    assert path.parents[0].name == source


def test_fetch_gtdb_genome_ids(setup_and_teardown):
    tmp_dir, script_file = setup_and_teardown

    release_ver = '207'
    genome_ids, _ = gtdb._fetch_gtdb_genome_ids(release_ver, tmp_dir)
    assert len(genome_ids) == 317542

    release_ver = '202'
    genome_ids, _ = gtdb._fetch_gtdb_genome_ids(release_ver, tmp_dir)
    assert len(genome_ids) == 258406


def test_parse_gtdb_release_vers():
    release_vers = gtdb._parse_gtdb_release_vers()

    expected_release_vers = ['207', '202', '95', '89', '86', '83', '80']

    assert set(expected_release_vers) <= set(release_vers)


def test_form_gtdb_taxonomy_file_url():
    file_urls_207 = gtdb._form_gtdb_taxonomy_file_url('207')
    expected_file_urls_207 = [f'{GTDB_DOMAIN}release207/207.0/ar53_taxonomy_r207.tsv',
                              f'{GTDB_DOMAIN}release207/207.0/bac120_taxonomy_r207.tsv']

    assert file_urls_207 == expected_file_urls_207

    file_urls_202 = gtdb._form_gtdb_taxonomy_file_url('202')
    expected_file_urls_202 = [f'{GTDB_DOMAIN}release202/202.0/ar122_taxonomy_r202.tsv',
                              f'{GTDB_DOMAIN}release202/202.0/bac120_taxonomy_r202.tsv']

    assert file_urls_202 == expected_file_urls_202

    file_urls_95 = gtdb._form_gtdb_taxonomy_file_url('95')
    expected_file_urls_95 = [f'{GTDB_DOMAIN}release95/95.0/ar122_taxonomy_r95.tsv',
                             f'{GTDB_DOMAIN}release95/95.0/bac120_taxonomy_r95.tsv']

    assert file_urls_95 == expected_file_urls_95

    file_urls_80 = gtdb._form_gtdb_taxonomy_file_url('80')
    expected_file_urls_80 = [f'{GTDB_DOMAIN}release80/80.0/bac_taxonomy_r80.tsv']

    assert file_urls_80 == expected_file_urls_80


def test_download_genome_file(setup_and_teardown):
    tmp_dir, script_file = setup_and_teardown
    test_genome_id = 'GCF_000979375.1'
    download_dir = os.path.join(tmp_dir, test_genome_id)
    os.makedirs(download_dir, exist_ok=True)

    ncbi_downloader_helper._download_genome_file(download_dir, test_genome_id,
                                          ['assembly_report.txt', 'md5checksums.txt'],
                                          ['checksums'])

    downloaded_files = os.listdir(download_dir)

    assert len(downloaded_files) == 1
    assert downloaded_files[0].endswith('assembly_report.txt')


def test_remove_ids_with_existing_data(setup_and_teardown):
    tmp_dir, script_file = setup_and_teardown
    work_dir = ncbi_downloader_helper.get_work_dir(tmp_dir)
    data_dir_1 = Path(os.path.join(work_dir, 'GCA_000172955.1'))
    data_dir_2 = Path(os.path.join(work_dir, 'GCF_000979585.1'))

    # create 2 genome folders
    os.makedirs(data_dir_1)
    os.makedirs(data_dir_2)

    # dir_1 has both assembly and genome files required while dir_2 is missing genome
    Path.touch(data_dir_1 / 'GCA_000172955.1_ASM17295v1_genomic.fna.gz')
    Path.touch(data_dir_1 / 'GCA_000172955.1_ASM17295v1_genomic.gbff.gz')
    Path.touch(data_dir_2 / 'GCF_000979585.1_gtlEnvA5udCFS_genomic.fna.gz')

    genome_ids = ncbi_downloader_helper.remove_ids_with_existing_data(
        tmp_dir,
        ['GCA_000172955.1', 'GCF_000979585.1'],
        ["genomic.fna.gz", "genomic.gbff.gz"],
        ['cds_from', 'rna_from', 'ERR'],
        False,
    )

    assert genome_ids == ['GCF_000979585.1']