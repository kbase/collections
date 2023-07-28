import os
import pytest
import shutil
import uuid

from pathlib import Path
from unittest.mock import Mock

from src.loaders.workspace_uploader import workspace_uploader

@pytest.fixture(scope="function")
def setup_and_teardown():
    print('starting workspace uploader tests')
    tmp_dir = 'result_{}'.format(str(uuid.uuid4()))
    os.makedirs(tmp_dir)

    # set up sourcedata and collectionssource dirs and soft link them
    sourcedata_dir = Path(tmp_dir) / "sourcedata" / "NCBI" / "NONE"
    collection_source_dir = Path(tmp_dir) / "collectionssource"
    sourcedata_dir.mkdir(parents=True)
    collection_source_dir.mkdir()

    assembly_dir_names = ["GCF_000979855.1", "GCF_000979175.1"]
    assembly_names = ["GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
                      "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz"]

    assembly_dirs, target_files = list(), list()
    for assembly_dir_name, assembly_name in zip(assembly_dir_names, assembly_names):
        target_dir_path = sourcedata_dir.joinpath(assembly_dir_name)
        target_dir_path.mkdir()
        target_file_path = target_dir_path.joinpath(assembly_name)
        target_file_path.touch()
        new_dir_path = collection_source_dir.joinpath(assembly_dir_name)
        os.symlink(target_dir_path.resolve(), new_dir_path.resolve(), target_is_directory=True)
        assembly_dirs.append(str(new_dir_path))
        target_files.append(str(target_file_path))

    yield tmp_dir, assembly_dirs, assembly_names, target_files

    shutil.rmtree(tmp_dir, ignore_errors=True)


def test_get_yaml_file_path(setup_and_teardown):
    tmp_dir, assembly_dirs, _, _ = setup_and_teardown
    assembly_dir_name = os.path.basename(assembly_dirs[0])
    yaml_path = workspace_uploader._get_yaml_file_path(tmp_dir, assembly_dir_name)

    assert Path(yaml_path).resolve().name == f'{workspace_uploader.UPLOADED_YAML}'
    assert os.path.exists(yaml_path)


def test_sanitize_data_dir(setup_and_teardown):
    tmp_dir, _, _, _ = setup_and_teardown

    data_dir = workspace_uploader._sanitize_data_dir(tmp_dir)

    path = Path(data_dir).resolve()

    assert path.name == f'{workspace_uploader.DATA_DIR}'
    assert os.listdir(data_dir) == []


def test_get_source_file(setup_and_teardown):
    _, assembly_dirs, assembly_names, target_files = setup_and_teardown
    assembly_dir, assembly_name = assembly_dirs[0], assembly_names[0]

    file_path = workspace_uploader._get_source_file(assembly_dir, assembly_name)
    expected_target_file_path = os.path.abspath(target_files[0])

    assert expected_target_file_path == file_path


def test_read_yaml_file(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, _ = setup_and_teardown
    workspace_id, env = 12345, "CI"
    assembly_dir_name = os.path.basename(assembly_dirs[0])
    assembly_name = assembly_names[0]

    data, uploaded = workspace_uploader._read_yaml_file(tmp_dir, env, workspace_id, assembly_dir_name, assembly_name)
    
    expected_data = {env: {workspace_id: list()}}
    
    assert not uploaded
    assert expected_data == data


def test_update_yaml_file(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, _ = setup_and_teardown
    workspace_id, env, upa = 12345, "CI", "12345_58_1"
    assembly_dir_name = os.path.basename(assembly_dirs[0])
    assembly_name = assembly_names[0]
    

    workspace_uploader._update_yaml_file(tmp_dir, env, workspace_id, upa, assembly_dir_name, assembly_name)
    data, uploaded = workspace_uploader._read_yaml_file(tmp_dir, env, workspace_id, assembly_dir_name, assembly_name)

    expected_data = {env: {workspace_id: [{"file_name": assembly_name, "upa": upa}]}}

    assert uploaded
    assert expected_data == data


def test_fetch_assemblies_to_upload(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, _ = setup_and_teardown
    workspace_id, env = 12345, "CI"
    collection_source_dir = Path(tmp_dir) / "collectionssource"
    upload_file_ext = ["genomic.fna.gz"]

    count, wait_to_upload_assemblies = workspace_uploader._fetch_assemblies_to_upload(
        tmp_dir, env, workspace_id, collection_source_dir, upload_file_ext
    )
    
    expected_count = len(assembly_names)
    expected_wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(assembly_names, assembly_dirs)
    }

    assert expected_count == count
    assert expected_wait_to_upload_assemblies == wait_to_upload_assemblies


def test_prepare_skd_job_dir_to_upload(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, _ = setup_and_teardown
    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(assembly_names, assembly_dirs)
    }

    data_dir = workspace_uploader._prepare_skd_job_dir_to_upload(tmp_dir, wait_to_upload_assemblies)
    
    path = Path(data_dir).resolve()
    assert path.name == f'{workspace_uploader.DATA_DIR}'
    assert sorted(os.listdir(data_dir)) == sorted(assembly_names)


def test_get_assembly_name_upa_mapping(setup_and_teardown):
    _, _, assembly_names, _ = setup_and_teardown
    workspace_id, objid_1, objid_2, version = 12345, 1, 2, 1
    conf = Mock()
    conf.ws.get_workspace_info.return_value = [
        workspace_id, 'sijiex:narrative_1688077625427', 'sijiex',
        '2023-07-27T00:00:00+0000', 2, 'a', 'r', 'unlocked',
        {'cell_count': '1', 'narrative_nice_name': 'workspace uploader testing',
        'searchtags': 'narrative', 'is_temporary': 'false', 'narrative': '1'}
    ]
    
    conf.ws.list_objects.return_value = [
        [objid_1, assembly_names[0], 'KBaseGenomeAnnotations.Assembly-6.3',
         '2023-07-26T23:59:43+0000', version, 'sijiex', workspace_id, 'sijiex:narrative_1688077625427',
         '33a0051edb784871a5c9ff349bac72d7', 38242, None],
        [objid_2, assembly_names[1], 'KBaseGenomeAnnotations.Assembly-6.3',
         '2023-07-27T00:00:00+0000', version, 'sijiex', workspace_id, 'sijiex:narrative_1688077625427',
         '0570f42ae543b94bbab941210430ee7b', 880, None]
    ]

    upa_1 = f'{workspace_id}_{objid_1}_{version}'
    upa_2 = f'{workspace_id}_{objid_2}_{version}'

    hashmap = workspace_uploader._get_assembly_name_upa_mapping(conf, workspace_id)
    expected_hashmap = {assembly_names[0]: upa_1, assembly_names[1]: upa_2}

    assert expected_hashmap == hashmap


def test_create_entries_in_sourcedata_workspace(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, _ = setup_and_teardown
    workspace_id, env = 12345, "CI"
    upas = ["12345_58_1", "12345_58_2"]
    output_dir = Path(tmp_dir) / "output_dir"
    output_dir.mkdir()

    assembly_name_to_dir = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(assembly_names, assembly_dirs)
    }
    assembly_name_to_upa = {
        assembly_name: upa
        for assembly_name, upa in zip(assembly_names, upas)
    }

    workspace_uploader.create_entries_in_sourcedata_workspace(
        tmp_dir, env, workspace_id, assembly_names, assembly_name_to_dir, assembly_name_to_upa, output_dir
    )

    for assembly_dir, assembly_name, upa in zip(assembly_dirs, assembly_names, upas):
        assembly_dir_name = os.path.basename(assembly_dir)
        data, uploaded = workspace_uploader._read_yaml_file(tmp_dir, env, workspace_id, assembly_dir_name, assembly_name)
        expected_data = {env: {workspace_id:[{"file_name": assembly_name, "upa": upa}]}}

        assert uploaded
        assert expected_data == data

    assert sorted(os.listdir(output_dir)) == sorted(upas)


def test_upload_assemblies_to_workspace(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, target_files = setup_and_teardown
    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(assembly_names, assembly_dirs)
    }

    workspace_name = "workspace_test"
    data_dir = workspace_uploader._prepare_skd_job_dir_to_upload(tmp_dir, wait_to_upload_assemblies)
    conf = Mock()
    conf.asu.save_assembly_from_fasta.return_value = None

    failed_names = workspace_uploader.upload_assemblies_to_workspace(conf, workspace_name, data_dir)
    expected_failed_names = list()

    assert expected_failed_names == failed_names
    assert conf.asu.save_assembly_from_fasta.call_count == len(target_files)
