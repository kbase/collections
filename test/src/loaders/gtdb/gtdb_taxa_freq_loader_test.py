import inspect
import os
import shutil
import subprocess
import uuid
from pathlib import Path

import jsonlines
import pytest


@pytest.fixture(scope="module")
def setup_and_teardown():
    print('starting GTDB taxa frequency test')
    tmp_dir = 'result_{}'.format(str(uuid.uuid4()))
    os.makedirs(tmp_dir, exist_ok=True)

    caller_frame = inspect.stack()[0]
    caller_filename_full = caller_frame.filename
    caller_file_dir = os.path.dirname(caller_filename_full)

    project_dir = Path(caller_file_dir).resolve().parents[3]
    script_file = '{}/src/loaders/gtdb/gtdb_taxa_freq_loader.py'.format(project_dir)

    yield tmp_dir, caller_file_dir, script_file

    shutil.rmtree(tmp_dir, ignore_errors=True)


def _exam_result_file(result_file, expected_docs_length, expected_doc_keys,
                      expected_release_version, expected_collection):
    with jsonlines.open(result_file, 'r') as jsonl_f:
        data = [obj for obj in jsonl_f]

    assert len(data) == expected_docs_length

    first_doc = data[0]
    assert set(first_doc.keys()) == expected_doc_keys

    versions = set([d['load_version'] for d in data])
    collections = set([d['collection'] for d in data])
    assert versions == {expected_release_version}
    assert collections == {expected_collection}


def _exe_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    print(str(stdout), str(stderr))


def test_create_json_default(setup_and_teardown):
    tmp_dir, caller_file_dir, script_file = setup_and_teardown

    result_file = os.path.join(tmp_dir, 'test.json')
    release_version = 100
    command = ['python', script_file,
               os.path.join(caller_file_dir, 'ar53_taxonomy_r207.tsv'),
               '--release_version', str(release_version),
               '-o', result_file]

    _exe_command(command)

    expected_docs_length = 5420
    expected_doc_keys = {'_key', 'collection', 'load_version', 'rank', 'name', 'count'}
    expected_collection = 'gtdb'
    _exam_result_file(result_file, expected_docs_length, expected_doc_keys,
                      release_version, expected_collection)


def test_create_json_option_input(setup_and_teardown):
    tmp_dir, caller_file_dir, script_file = setup_and_teardown

    result_file = os.path.join(tmp_dir, 'test2.json')
    release_version = 300
    kbase_collections = 'test_gtdb'
    command = ['python', script_file,
               os.path.join(caller_file_dir, 'ar53_taxonomy_r207.tsv'),
               os.path.join(caller_file_dir, 'ar53_taxonomy_r207.tsv'),
               '--release_version', str(release_version),
               '--output', result_file,
               '--kbase_collection', kbase_collections]

    _exe_command(command)

    expected_docs_length = 5420
    expected_doc_keys = {'_key', 'collection', 'load_version', 'rank', 'name', 'count'}
    _exam_result_file(result_file, expected_docs_length, expected_doc_keys,
                      release_version, kbase_collections)
