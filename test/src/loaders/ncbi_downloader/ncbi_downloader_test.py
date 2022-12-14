import os
import shutil
import uuid
from pathlib import Path

import pytest

from src.loaders.common.loader_common_names import SOURCE_DATA_DIR
from src.loaders.ncbi_downloader import ncbi_downloader
from src.loaders.ncbi_downloader.ncbi_downloader import GTDB_DOMAIN


@pytest.fixture(scope="module")
def setup_and_teardown():
    print('starting NCBI downloader tests')
    tmp_dir = 'result_{}'.format(str(uuid.uuid4()))
    os.makedirs(tmp_dir, exist_ok=True)

    caller_filename_full = Path(__file__).resolve()
    caller_file_dir = os.path.dirname(caller_filename_full)

    project_dir = Path(caller_file_dir).resolve().parents[3]
    script_file = f'{project_dir}/src/loaders/ncbi_downloader/ncbi_downloader.py'

    yield tmp_dir, script_file

    shutil.rmtree(tmp_dir, ignore_errors=True)


def test_make_work_dir(setup_and_teardown):
    tmp_dir, script_file = setup_and_teardown

    with pytest.raises(ValueError, match='Unexpected source:'):
        fake_source = 'hello_fake'
        ncbi_downloader._make_work_dir(tmp_dir, SOURCE_DATA_DIR, fake_source, 'release_ver')

    source, release_ver = 'GTDB', '207'
    work_dir = ncbi_downloader._make_work_dir(tmp_dir, SOURCE_DATA_DIR, source, release_ver)

    path = Path(work_dir).resolve()

    assert path.name == f'r{release_ver}'
    assert path.parents[0].name == source


def test_fetch_gtdb_genome_ids(setup_and_teardown):
    tmp_dir, script_file = setup_and_teardown

    release_ver = '207'
    genome_ids = ncbi_downloader._fetch_gtdb_genome_ids(release_ver, tmp_dir)
    assert len(genome_ids) == 317542

    release_ver = '202'
    genome_ids = ncbi_downloader._fetch_gtdb_genome_ids(release_ver, tmp_dir)
    assert len(genome_ids) == 258406


def test_parse_gtdb_release_vers():
    release_vers = ncbi_downloader._parse_gtdb_release_vers()

    expected_release_vers = ['207', '202', '95', '89', '86', '83', '80']

    assert set(expected_release_vers) <= set(release_vers)


def test_form_gtdb_taxonomy_file_url():
    file_urls_207 = ncbi_downloader._form_gtdb_taxonomy_file_url('207')
    expected_file_urls_207 = [f'{GTDB_DOMAIN}release207/207.0/ar53_taxonomy_r207.tsv',
                              f'{GTDB_DOMAIN}release207/207.0/bac120_taxonomy_r207.tsv']

    assert file_urls_207 == expected_file_urls_207

    file_urls_202 = ncbi_downloader._form_gtdb_taxonomy_file_url('202')
    expected_file_urls_202 = [f'{GTDB_DOMAIN}release202/202.0/ar122_taxonomy_r202.tsv',
                              f'{GTDB_DOMAIN}release202/202.0/bac120_taxonomy_r202.tsv']

    assert file_urls_202 == expected_file_urls_202

    file_urls_95 = ncbi_downloader._form_gtdb_taxonomy_file_url('95')
    expected_file_urls_95 = [f'{GTDB_DOMAIN}release95/95.0/ar122_taxonomy_r95.tsv',
                             f'{GTDB_DOMAIN}release95/95.0/bac120_taxonomy_r95.tsv']

    assert file_urls_95 == expected_file_urls_95

    file_urls_80 = ncbi_downloader._form_gtdb_taxonomy_file_url('80')
    expected_file_urls_80 = [f'{GTDB_DOMAIN}release80/80.0/bac_taxonomy_r80.tsv']

    assert file_urls_80 == expected_file_urls_80


def test_download_genome_file(setup_and_teardown):
    tmp_dir, script_file = setup_and_teardown
    test_genome_id = 'GCF_000979375.1'
    download_dir = os.path.join(tmp_dir, test_genome_id)
    os.makedirs(download_dir, exist_ok=True)

    ncbi_downloader._download_genome_file(download_dir, test_genome_id,
                                          ['assembly_report.txt', 'md5checksums.txt'],
                                          ['checksums'])

    downloaded_files = os.listdir(download_dir)

    assert len(downloaded_files) == 1
    assert downloaded_files[0].endswith('assembly_report.txt')

