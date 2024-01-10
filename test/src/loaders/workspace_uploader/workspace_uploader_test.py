import os
import shutil
import uuid
from multiprocessing import Queue
from pathlib import Path
from typing import NamedTuple
from unittest.mock import Mock, create_autospec

import pytest

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.loaders.common import loader_helper
from src.loaders.common.callback_server_wrapper import Conf
from src.loaders.workspace_uploader import workspace_uploader

ASSEMBLY_DIR_NAMES = ["GCF_000979855.1", "GCF_000979175.1"]
ASSEMBLY_NAMES = [
    "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
    "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
]


class Params(NamedTuple):
    tmp_dir: str
    sourcedata_dir: str
    collection_source_dir: str
    assembly_dirs: list[str]
    target_files: list[str]


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

    yield Params(
        tmp_dir, sourcedata_dir, collection_source_dir, assembly_dirs, target_files
    )

    shutil.rmtree(tmp_dir, ignore_errors=True)


def test_get_yaml_file_path(setup_and_teardown):
    params = setup_and_teardown
    assembly_dir = params.assembly_dirs[0]
    yaml_path = workspace_uploader._get_yaml_file_path(assembly_dir)

    expected_yaml_path = os.path.join(assembly_dir, workspace_uploader.UPLOADED_YAML)
    assert expected_yaml_path == yaml_path
    assert os.path.exists(yaml_path)


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

    # test empty yaml file in assembly_dir
    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 12345, assembly_dir, assembly_name
    )

    expected_data = {"CI": {12345: dict()}}

    assert not uploaded
    assert expected_data == data


def test_update_upload_status_yaml_file(setup_and_teardown):
    params = setup_and_teardown
    assembly_dir = params.assembly_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]

    workspace_uploader._update_upload_status_yaml_file(
        "CI", 12345, "12345_58_1", assembly_dir, assembly_name
    )
    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 12345, assembly_dir, assembly_name
    )

    expected_data = {"CI": {12345: {"file_name": assembly_name, "upa": "12345_58_1"}}}

    assert uploaded
    assert expected_data == data

    with pytest.raises(ValueError, match=f"already exists in workspace"):
        workspace_uploader._update_upload_status_yaml_file(
            "CI", 12345, "12345_58_1", assembly_dir, assembly_name
        )


def test_fetch_assemblies_to_upload(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs
    collection_source_dir = params.collection_source_dir

    count, wait_to_upload_assemblies = workspace_uploader._fetch_assemblies_to_upload(
        "CI",
        12345,
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

    # let's assume these two assemly files are uploaded successfully
    # Each UPLOADED_YAML file is also updated with the upa assigned from the workspace service
    # Both assemnly files will be skipped in the next fetch_assemblies_to_upload call
    upas = ["12345_58_1", "12345_58_2"]
    for assembly_name, assembly_dir, upa in zip(ASSEMBLY_NAMES, assembly_dirs, upas):
        workspace_uploader._update_upload_status_yaml_file(
            "CI", 12345, upa, assembly_dir, assembly_name
        )

    (
        new_count,
        new_wait_to_upload_assemblies,
    ) = workspace_uploader._fetch_assemblies_to_upload(
        "CI",
        12345,
        collection_source_dir,
        workspace_uploader.UPLOAD_FILE_EXT,
    )

    assert expected_count == new_count
    assert {} == new_wait_to_upload_assemblies


def test_prepare_skd_job_dir_to_upload(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs
    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }

    conf = Mock()
    job_dir = loader_helper.make_job_dir(params.tmp_dir, "kbase")
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

    host_assembly_dir = params.assembly_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]
    src_file = params.target_files[0]

    workspace_uploader._post_process(
        "CI",
        88888,
        host_assembly_dir,
        assembly_name,
        upload_dir,
        output_dir,
        "12345_58_1",
    )

    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 88888, host_assembly_dir, assembly_name
    )
    expected_data = {"CI": {88888: {"file_name": assembly_name, "upa": "12345_58_1"}}}

    dest_file = os.path.join(
        os.path.join(output_dir, "12345_58_1"), f"12345_58_1.fna.gz"
    )

    assert uploaded
    assert expected_data == data
    # check softlink
    assert os.readlink(os.path.join(upload_dir, "12345_58_1")) == os.path.join(
        output_dir, "12345_58_1"
    )
    # check hardlink
    assert os.path.samefile(src_file, dest_file)


def test_upload_assembly_to_workspace(setup_and_teardown):
    _ = setup_and_teardown
    assembly_name = ASSEMBLY_NAMES[0]

    conf = Mock()
    conf.asu = create_autospec(AssemblyUtil, spec_set=True, instance=True)
    conf.asu.save_assembly_from_fasta2.return_value = {"upa": "12345/58/1"}
    upa = workspace_uploader._upload_assembly_to_workspace(
        conf, 12345, "/path/to/file/in/AssembilyUtil", assembly_name
    )

    assert upa == "12345_58_1"
    conf.asu.save_assembly_from_fasta2.assert_called_once_with(
        {
            "file": {"path": "/path/to/file/in/AssembilyUtil"},
            "workspace_id": 12345,
            "assembly_name": assembly_name,
        }
    )


def test_assembly_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    upload_dir = Path(params.tmp_dir) / "upload_dir"
    upload_dir.mkdir()
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
    conf.output_queue.put((ASSEMBLY_NAMES[0], "12345_58_1"))
    # an upload failed
    conf.output_queue.put((ASSEMBLY_NAMES[1], None))

    failed_names = workspace_uploader._upload_assembly_files_in_parallel(
        conf, "CI", 12345, upload_dir, wait_to_upload_assemblies
    )

    expected_tuple1 = (
        "CI",
        12345,
        os.path.join(
            workspace_uploader.JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, ASSEMBLY_NAMES[0]
        ),
        assembly_dirs[0],
        ASSEMBLY_NAMES[0],
        upload_dir,
        1,
        len(ASSEMBLY_NAMES),
    )

    expected_tuple2 = (
        "CI",
        12345,
        os.path.join(
            workspace_uploader.JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER, ASSEMBLY_NAMES[1]
        ),
        assembly_dirs[1],
        ASSEMBLY_NAMES[1],
        upload_dir,
        2,
        len(ASSEMBLY_NAMES),
    )

    assert conf.input_queue.get() == expected_tuple1
    assert conf.input_queue.get() == expected_tuple2
    assert conf.output_queue.empty()
    assert failed_names == [ASSEMBLY_NAMES[1]]
