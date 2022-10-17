import inspect
import json
import os
import shutil
import subprocess
import unittest
import uuid
from pathlib import Path


class TaxaFrequencyTest(unittest.TestCase):

    def setUp(self) -> None:
        print('starting GTDB taxa frequency test')
        self.tmp_dir = 'result_{}'.format(str(uuid.uuid4()))
        os.makedirs(self.tmp_dir, exist_ok=True)

        caller_frame = inspect.stack()[0]
        caller_filename_full = caller_frame.filename
        self.caller_file_dir = os.path.dirname(caller_filename_full)

        project_dir = Path(self.caller_file_dir).resolve().parents[3]
        self.script_file = '{}/src/loaders/gtdb/gtdb_taxa_freq_loader.py'.format(project_dir)

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    def test_create_json_default(self):
        result_file = os.path.join(self.tmp_dir, 'test.json')
        release_version = 100
        command = ['python', self.script_file,
                   os.path.join(self.caller_file_dir, 'ar53_taxonomy_r207.tsv'),
                   str(release_version),
                   '-o', result_file]

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = p.communicate()
        print(str(stdout), str(stderr))
        with open(result_file, 'r') as f:
            data = json.load(f)

        expected_docs_length = 5420
        assert len(data) == expected_docs_length

        first_doc = data[0]
        expected_keys = {'_key', 'collection', 'load_version', 'rank', 'name', 'count'}
        self.assertCountEqual(first_doc.keys(), expected_keys)

        versions = set([d['load_version'] for d in data])
        collections = set([d['collection'] for d in data])
        assert versions == {release_version}
        assert collections == {'gtdb'}

    def test_create_json_option_input(self):
        result_file = os.path.join(self.tmp_dir, 'test2.json')
        release_version = 300
        kbase_collections = 'test_gtdb'
        command = ['python', self.script_file,
                   os.path.join(self.caller_file_dir, 'ar53_taxonomy_r207.tsv'),
                   os.path.join(self.caller_file_dir, 'ar53_taxonomy_r207.tsv'),
                   str(release_version),
                   '--output', result_file,
                   '--kbase_collection', kbase_collections]

        p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        stdout, stderr = p.communicate()
        print(str(stdout), str(stderr))
        with open(result_file, 'r') as f:
            data = json.load(f)

        expected_docs_length = 5420
        assert len(data) == expected_docs_length

        first_doc = data[0]
        expected_keys = {'_key', 'collection', 'load_version', 'rank', 'name', 'count'}
        self.assertCountEqual(first_doc.keys(), expected_keys)

        versions = set([d['load_version'] for d in data])
        collections = set([d['collection'] for d in data])
        assert versions == {release_version}
        assert collections == {kbase_collections}



