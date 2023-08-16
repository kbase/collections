import os
import pytest
import shutil
import uuid
from multiprocessing import Queue

from pathlib import Path
from unittest.mock import Mock

from src.loaders.common import loader_helper
from src.loaders.workspace_uploader import workspace_uploader


@pytest.fixture(scope="function")
def setup_and_teardown():
    print('starting workspace uploader tests')
    tmp_dir = 'result_{}'.format(uuid.uuid4().hex)
    os.makedirs(tmp_dir)

    # set up sourcedata and collectionssource dirs and soft link them
    sourcedata_dir = Path(tmp_dir) / "sourcedata" / "NCBI" / "NONE"
    collection_source_dir = Path(tmp_dir) / "collectionssource"
    sourcedata_dir.mkdir(parents=True)
    collection_source_dir.mkdir()

    workspace_id = 12345
    upload_env_key = "CI"
    upas = ["12345_58_1", "12345_58_2"]
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

    yield tmp_dir, assembly_dirs, assembly_names, upas, workspace_id, upload_env_key, target_files

    shutil.rmtree(tmp_dir, ignore_errors=True)


def test_get_yaml_file_path(setup_and_teardown):
    _, assembly_dirs, _, _, _, _, _ = setup_and_teardown
    yaml_path = workspace_uploader._get_yaml_file_path(assembly_dirs[0])

    assert os.path.exists(yaml_path)


def test_get_source_file(setup_and_teardown):
    _, assembly_dirs, assembly_names, _, _, _, target_files = setup_and_teardown
    assembly_dir, assembly_name = assembly_dirs[0], assembly_names[0]

    file_path = workspace_uploader._get_source_file(assembly_dir, assembly_name)
    expected_target_file_path = os.path.abspath(target_files[0])

    assert expected_target_file_path == file_path


def test_read_upload_status_yaml_file(setup_and_teardown):
    _, assembly_dirs, assembly_names, _, workspace_id, upload_env_key, _ = setup_and_teardown
    assembly_dir = assembly_dirs[0]
    assembly_name = assembly_names[0]

    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        upload_env_key, workspace_id, assembly_dir, assembly_name
    )
    
    expected_data = {upload_env_key: {workspace_id: dict()}}
    
    assert not uploaded
    assert expected_data == data


def test_update_upload_status_yaml_file(setup_and_teardown):
    _, assembly_dirs, assembly_names, upas, workspace_id, upload_env_key, _ = setup_and_teardown
    upa = upas[0]
    assembly_dir = assembly_dirs[0]
    assembly_name = assembly_names[0]
    
    workspace_uploader._update_upload_status_yaml_file(
        upload_env_key, workspace_id, upa, assembly_dir, assembly_name
    )
    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        upload_env_key, workspace_id, assembly_dir, assembly_name
    )

    expected_data = {
        upload_env_key: {workspace_id: {"file_name": assembly_name, "upa": upa}}
    }

    assert uploaded
    assert expected_data == data


def test_fetch_assemblies_to_upload(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, _, workspace_id, upload_env_key, _ = setup_and_teardown
    collection_source_dir = Path(tmp_dir) / "collectionssource"
    upload_file_ext = ["genomic.fna.gz"]

    count, wait_to_upload_assemblies = workspace_uploader._fetch_assemblies_to_upload(
        upload_env_key, workspace_id, collection_source_dir, upload_file_ext
    )
    
    expected_count = len(assembly_names)
    expected_wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(assembly_names, assembly_dirs)
    }

    assert expected_count == count
    assert expected_wait_to_upload_assemblies == wait_to_upload_assemblies


def test_prepare_skd_job_dir_to_upload(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, _, _, _, _ = setup_and_teardown
    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(assembly_names, assembly_dirs)
    }

    conf = Mock()
    job_dir = loader_helper.make_job_dir(tmp_dir, os.getlogin())
    conf.job_data_dir = loader_helper.make_job_data_dir(job_dir)
    data_dir = workspace_uploader._prepare_skd_job_dir_to_upload(conf, wait_to_upload_assemblies)
    
    assert sorted(os.listdir(data_dir)) == sorted(assembly_names)


def test_post_process(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, upas, workspace_id, upload_env_key, _ = setup_and_teardown
    upload_dir = Path(tmp_dir) / "upload_dir"
    upload_dir.mkdir()
    output_dir = Path(tmp_dir) / "output_dir"
    output_dir.mkdir()

    upa = upas[0]
    host_assembly_dir = assembly_dirs[0]
    assembly_name = assembly_names[0]

    workspace_uploader._post_process(
        upload_env_key,
        workspace_id,
        host_assembly_dir,
        assembly_name,
        upload_dir,
        output_dir,
        upa,
    )

    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        upload_env_key, workspace_id, host_assembly_dir, assembly_name
    )
    expected_data = {
        upload_env_key: {workspace_id: {"file_name": assembly_name, "upa": upa}}
    }

    assert uploaded
    assert expected_data == data
    assert os.readlink(os.path.join(upload_dir, upa)) == os.path.join(output_dir, upa)


def test_upload_assembly_to_workspace(setup_and_teardown):
    _, _, assembly_names, upas, workspace_id, _, _  = setup_and_teardown
    upa = upas[0]
    assembly_name = assembly_names[0]
    file_path = "/path/to/file/in/AssembilyUtil"

    conf = Mock()
    conf.asu.save_assembly_from_fasta2.return_value = {"upa": upa}
    upa_res = workspace_uploader._upload_assembly_to_workspace(
        conf, workspace_id, file_path, assembly_name
    )

    expected_upa = upa.replace("/", "_")
    assert expected_upa == upa_res
    assert conf.asu.save_assembly_from_fasta2.call_count == 1


def test_assembly_files_in_parallel(setup_and_teardown):
    tmp_dir, assembly_dirs, assembly_names, upas, workspace_id, upload_env_key, _ = setup_and_teardown
    upload_dir = Path(tmp_dir) / "upload_dir"
    upload_dir.mkdir()

    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(assembly_names, assembly_dirs)
    }

    conf = Mock()
    conf.workers = 5
    conf.input_queue = Queue()
    conf.output_queue = Queue()
    for assembly_name, upa in zip(assembly_names, upas):
        conf.output_queue.put((assembly_name, upa))
    
    failed_names = workspace_uploader._upload_assembly_files_in_parallel(
        conf, upload_env_key, workspace_id, upload_dir, wait_to_upload_assemblies
    )

    assert conf.output_queue.empty()
    assert failed_names == []
