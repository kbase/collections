import os
import shutil
import uuid
from collections import namedtuple
from multiprocessing import Queue
from pathlib import Path
from unittest.mock import Mock, create_autospec

import pytest

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.loaders.common import loader_helper
from src.loaders.workspace_downloader.workspace_downloader_helper import Conf
from src.loaders.workspace_uploader import workspace_uploader

ASSEMBLY_DIR_NAMES = ["GCF_000979855.1", "GCF_000979175.1"]
ASSEMBLY_NAMES = [
    "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
    "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
]


@pytest.fixture(scope="function")
def setup_and_teardown():
    print("starting workspace uploader tests")
    tmp_dir = "result_{}".format(uuid.uuid4().hex)
    os.makedirs(tmp_dir)

    # set up sourcedata and collectionssource dirs and soft link them
    sourcedata_dir = Path(tmp_dir) / "sourcedata" / "NCBI" / "NONE"
    collection_source_dir = Path(tmp_dir) / "collectionssource"
    sourcedata_dir.mkdir(parents=True)
    collection_source_dir.mkdir()

    assembly_dirs, target_files = list(), list()
    for assembly_dir_name, assembly_name in zip(ASSEMBLY_DIR_NAMES, ASSEMBLY_NAMES):
        target_dir_path = sourcedata_dir.joinpath(assembly_dir_name)
        target_dir_path.mkdir()
        target_file_path = target_dir_path.joinpath(assembly_name)
        target_file_path.touch()
        new_dir_path = collection_source_dir.joinpath(assembly_dir_name)
        os.symlink(
            target_dir_path.resolve(), new_dir_path.resolve(), target_is_directory=True
        )
        assembly_dirs.append(str(new_dir_path))
        target_files.append(str(target_file_path))

    ParamsTuple = namedtuple(
        "ParamsTuple",
        [
            "tmp_dir",
            "sourcedata_dir",
            "collection_source_dir",
            "assembly_dirs",
            "target_files",
        ],
    )

    yield ParamsTuple(
        tmp_dir, sourcedata_dir, collection_source_dir, assembly_dirs, target_files
    )

    shutil.rmtree(tmp_dir, ignore_errors=True)


def test_get_yaml_file_path(setup_and_teardown):
    params = setup_and_teardown
    assembly_dir = params.assembly_dirs[0]
    yaml_path = workspace_uploader._get_yaml_file_path(assembly_dir)

    expected_yaml_path = os.path.join(assembly_dir, workspace_uploader.UPLOADED_YAML)
    assert expected_yaml_path == yaml_path


def test_get_source_file(setup_and_teardown):
    params = setup_and_teardown
    assembly_dir, assembly_name = params.assembly_dirs[0], ASSEMBLY_NAMES[0]
    file_path = workspace_uploader._get_source_file(assembly_dir, assembly_name)

    expected_target_file_path = os.path.abspath(params.target_files[0])
    assert expected_target_file_path == file_path


def test_read_upload_status_yaml_file(setup_and_teardown):
    params = setup_and_teardown
    assembly_dir = params.assembly_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]

    upload_env_key = "CI"
    workspace_id = 12345

    # test empty yaml file in assembly_dir
    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        upload_env_key, workspace_id, assembly_dir, assembly_name
    )

    expected_data = {upload_env_key: {workspace_id: dict()}}

    assert not uploaded
    assert expected_data == data


def test_update_upload_status_yaml_file(setup_and_teardown):
    params = setup_and_teardown
    upload_env_key = "CI"
    upa = "12345_58_1"
    workspace_id = 12345
    assembly_dir = params.assembly_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]

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

    with pytest.raises(ValueError, match=f"already exists in workspace"):
        workspace_uploader._update_upload_status_yaml_file(
            upload_env_key, workspace_id, upa, assembly_dir, assembly_name
        )


def test_fetch_assemblies_to_upload(setup_and_teardown):
    params = setup_and_teardown
    upload_env_key = "CI"
    workspace_id = 12345
    assembly_dirs = params.assembly_dirs
    collection_source_dir = params.collection_source_dir

    count, wait_to_upload_assemblies = workspace_uploader._fetch_assemblies_to_upload(
        upload_env_key,
        workspace_id,
        collection_source_dir,
        workspace_uploader.UPLOAD_FILE_EXT,
    )

    expected_count = len(ASSEMBLY_NAMES)
    expected_wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }

    assert expected_count == count
    assert expected_wait_to_upload_assemblies == wait_to_upload_assemblies


def test_prepare_skd_job_dir_to_upload(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs
    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }

    conf = Mock()
    username = "kbase"
    job_dir = loader_helper.make_job_dir(params.tmp_dir, username)
    conf.job_data_dir = loader_helper.make_job_data_dir(job_dir)
    data_dir = workspace_uploader._prepare_skd_job_dir_to_upload(
        conf, wait_to_upload_assemblies
    )

    assert sorted(os.listdir(data_dir)) == sorted(ASSEMBLY_NAMES)
    for assembly_name, src_file in zip(ASSEMBLY_NAMES, params.target_files):
        assert os.path.samefile(src_file, os.path.join(data_dir, assembly_name))


def test_post_process(setup_and_teardown):
    params = setup_and_teardown
    upload_dir = Path(params.tmp_dir) / "upload_dir"
    upload_dir.mkdir()
    output_dir = Path(params.tmp_dir) / "output_dir"
    output_dir.mkdir()

    upa = "12345_58_1"
    upload_env_key = "CI"
    workspace_id = 88888
    host_assembly_dir = params.assembly_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]
    src_file = params.target_files[0]

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

    dest_file = os.path.join(os.path.join(output_dir, upa), f"{upa}.fna.gz")

    assert uploaded
    assert expected_data == data
    # check softlink
    assert os.readlink(os.path.join(upload_dir, upa)) == os.path.join(output_dir, upa)
    # check hardlink
    assert os.path.samefile(src_file, dest_file)


def test_upload_assembly_to_workspace(setup_and_teardown):
    params = setup_and_teardown
    workspace_id = 12345
    assembly_name = ASSEMBLY_NAMES[0]
    file_path = "/path/to/file/in/AssembilyUtil"

    conf = create_autospec(Conf)
    conf.asu = create_autospec(AssemblyUtil)
    conf.asu.save_assembly_from_fasta2.return_value = {"upa": "12345/58/1"}
    upa = workspace_uploader._upload_assembly_to_workspace(
        conf, workspace_id, file_path, assembly_name
    )

    assert upa == "12345_58_1"
    assert conf.asu.save_assembly_from_fasta2.call_count == 1


def test_assembly_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    upload_dir = Path(params.tmp_dir) / "upload_dir"
    upload_dir.mkdir()

    upload_env_key = "CI"
    workspace_id = 12345
    upa = "12345_58_1"
    assembly_dirs = params.assembly_dirs

    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }

    conf = Mock()
    conf.workers = 5
    conf.input_queue = Queue()
    conf.output_queue = Queue()

    # an uploaded successful
    conf.output_queue.put((ASSEMBLY_NAMES[0], upa))
    # an upload failed
    conf.output_queue.put((ASSEMBLY_NAMES[1], None))

    failed_names = workspace_uploader._upload_assembly_files_in_parallel(
        conf, upload_env_key, workspace_id, upload_dir, wait_to_upload_assemblies
    )

    (
        q1_upload_env_key,
        q1_workspace_id,
        q1_container_internal_assembly_path,
        q1_host_assembly_dir,
        q1_assembly_name,
        q1_upload_dir,
        q1_counter,
        q1_assembly_files_len,
    ) = conf.input_queue.get()

    (
        q2_upload_env_key,
        q2_workspace_id,
        q2_container_internal_assembly_path,
        q2_host_assembly_dir,
        q2_assembly_name,
        q2_upload_dir,
        q2_counter,
        q2_assembly_files_len,
    ) = conf.input_queue.get()

    assert q1_upload_env_key == q2_upload_env_key == upload_env_key
    assert q1_workspace_id == q2_workspace_id == workspace_id
    assert q1_container_internal_assembly_path == os.path.join(
        workspace_uploader.JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, ASSEMBLY_NAMES[0]
    )
    assert q2_container_internal_assembly_path == os.path.join(
        workspace_uploader.JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, ASSEMBLY_NAMES[1]
    )
    assert q1_host_assembly_dir == assembly_dirs[0]
    assert q2_host_assembly_dir == assembly_dirs[1]
    assert q1_assembly_name == ASSEMBLY_NAMES[0]
    assert q2_assembly_name == ASSEMBLY_NAMES[1]
    assert q1_upload_dir == q2_upload_dir == upload_dir
    assert q1_counter == 1
    assert q2_counter == 2
    assert q1_assembly_files_len == q2_assembly_files_len == len(ASSEMBLY_NAMES)
    assert conf.output_queue.empty()
    assert failed_names == [ASSEMBLY_NAMES[1]]
