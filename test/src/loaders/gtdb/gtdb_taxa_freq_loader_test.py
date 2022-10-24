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
    script_file = f'{project_dir}/src/loaders/gtdb/gtdb_taxa_freq_loader.py'

    yield tmp_dir, caller_file_dir, script_file

    shutil.rmtree(tmp_dir, ignore_errors=True)


def _exam_freq_result_file(result_file, expected_docs_length, expected_doc_keys,
                           expected_load_version, expected_collection):
    with jsonlines.open(result_file, 'r') as jsonl_f:
        data = [obj for obj in jsonl_f]

    assert len(data) == expected_docs_length

    first_doc = data[0]
    assert set(first_doc.keys()) == expected_doc_keys

    versions = set([d['load_version'] for d in data])
    collections = set([d['collection'] for d in data])
    assert versions == {expected_load_version}
    assert collections == {expected_collection}


def _exam_rank_result_file(result_file, expected_load_version, expected_collection, expected_ranks_inorder):
    root_ext = os.path.splitext(result_file)
    rank_result_file = root_ext[0] + '_rank' + root_ext[1]

    with jsonlines.open(rank_result_file, 'r') as jsonl_f:
        data = [obj for obj in jsonl_f]

    assert len(data) == 1

    first_doc = data[0]
    assert set(first_doc.keys()) == {'_key', 'collection', 'load_version', 'ranks'}
    assert first_doc['load_version'] == expected_load_version
    assert first_doc['collection'] == expected_collection
    assert len(first_doc['ranks']) == len(expected_ranks_inorder)
    assert all([a == b for a, b in zip(first_doc['ranks'], expected_ranks_inorder)])


def _exe_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    print(str(stdout), str(stderr))


def test_create_json_default(setup_and_teardown):
    tmp_dir, caller_file_dir, script_file = setup_and_teardown

    result_file = os.path.join(tmp_dir, 'test.json')
    load_version = '100-dev'
    command = ['python', script_file,
               os.path.join(caller_file_dir, 'ar53_taxonomy_r207.tsv'),
               '--load_version', load_version,
               '-o', result_file]

    _exe_command(command)

    expected_docs_length = 5420
    expected_doc_keys = {'_key', 'collection', 'load_version', 'rank', 'name', 'count'}
    expected_collection = 'gtdb'
    _exam_freq_result_file(result_file, expected_docs_length, expected_doc_keys,
                           load_version, expected_collection)
    expected_ranks_inorder = ['domain', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    _exam_rank_result_file(result_file, load_version, expected_collection, expected_ranks_inorder)


def test_create_json_option_input(setup_and_teardown):
    tmp_dir, caller_file_dir, script_file = setup_and_teardown

    result_file = os.path.join(tmp_dir, 'test2.json')
    load_version = '300-beta'
    kbase_collections = 'test_gtdb'
    command = ['python', script_file,
               os.path.join(caller_file_dir, 'ar53_taxonomy_r207.tsv'),
               os.path.join(caller_file_dir, 'ar53_taxonomy_r207.tsv'),
               '--load_version', load_version,
               '--output', result_file,
               '--kbase_collection', kbase_collections]

    _exe_command(command)

    expected_docs_length = 5420
    expected_doc_keys = {'_key', 'collection', 'load_version', 'rank', 'name', 'count'}
    _exam_freq_result_file(result_file, expected_docs_length, expected_doc_keys,
                           load_version, kbase_collections)
    expected_ranks_inorder = ['domain', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    _exam_rank_result_file(result_file, load_version, kbase_collections, expected_ranks_inorder)
