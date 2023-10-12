import os
import shutil
import subprocess
import uuid
from pathlib import Path

import jsonlines
import pytest

import src.common.storage.collection_and_field_names as names
import src.loaders.gtdb.gtdb_genome_attribs_loader as loader
from src.loaders.common.loader_common_names import DEFAULT_ENV, IMPORT_DIR


@pytest.fixture(scope="module")
def setup_and_teardown():
    print('starting GTDB genome statistics test')
    tmp_dir = 'result_{}'.format(str(uuid.uuid4()))
    os.makedirs(tmp_dir, exist_ok=True)

    caller_filename_full = Path(__file__).resolve()
    caller_file_dir = os.path.dirname(caller_filename_full)

    project_dir = Path(caller_file_dir).resolve().parents[3]
    script_file = f'{project_dir}/src/loaders/gtdb/gtdb_genome_attribs_loader.py'

    yield tmp_dir, caller_file_dir, script_file

    shutil.rmtree(tmp_dir, ignore_errors=True)


def _exam_genome_attribs_file(root_dir, expected_docs_length, expected_doc_keys,
                              expected_load_version, expected_collection):

    result_file = os.path.join(root_dir, IMPORT_DIR, DEFAULT_ENV,
                               f'{expected_collection}_{expected_load_version}_{loader.GTDB_GENOME_ATTR_FILE}')
    with jsonlines.open(result_file, 'r') as jsonl_f:
        data = [obj for obj in jsonl_f]

    assert len(data) == expected_docs_length

    first_doc = data[0]
    assert set(first_doc.keys()) > expected_doc_keys

    versions = set([d['load_ver'] for d in data])
    collections = set([d['coll'] for d in data])
    assert versions == {expected_load_version}
    assert collections == {expected_collection}

    assert all([d[names.FLD_KBASE_ID] == d[loader.KBASE_GENOME_ID_COL] for d in data])


def _exe_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    print(str(stdout), str(stderr))


def test_create_json_default(setup_and_teardown):
    tmp_dir, caller_file_dir, script_file = setup_and_teardown

    result_file = os.path.join(tmp_dir, 'test.json')
    load_version = '100-dev'
    kbase_collections = 'GTDB'
    command = ['python', script_file,
               os.path.join(caller_file_dir, 'SAMPLE_ar53_metadata_r207.tsv'),
               os.path.join(caller_file_dir, 'SAMPLE_bac120_metadata_r207.tsv'),
               '--load_ver', load_version,
               '--kbase_collection', kbase_collections,
               '--root_dir', tmp_dir]

    _exe_command(command)

    expected_docs_length = 20
    expected_doc_keys = {names.FLD_KBASE_ID,  # sort key must exist
                         loader.KBASE_GENOME_ID_COL,
                         '_key', 'coll', 'load_ver', 'checkm_completeness',
                         'trna_selenocysteine_count', 'n50_scaffolds'}  # cherry-pick a few from SELECTED_FEATURES

    _exam_genome_attribs_file(tmp_dir, expected_docs_length, expected_doc_keys,
                              load_version, kbase_collections)
