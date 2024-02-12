import os
import shutil
import uuid
from pathlib import Path
from typing import NamedTuple
from unittest.mock import Mock, create_autospec

import pytest

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_helper
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

    expected_yaml_path = os.path.join(assembly_dir, workspace_uploader._UPLOADED_YAML)
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
        "CI", 12345, "214", assembly_dir
    )

    expected_data = {
        "CI": {12345: {}}}

    assert not uploaded
    assert expected_data == data


def test_update_upload_status_yaml_file(setup_and_teardown):
    params = setup_and_teardown
    assembly_dir = params.assembly_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]
    assembly_tuple = workspace_uploader._WSObjTuple(
        assembly_name, assembly_dir, "/path/to/file/in/AssembilyUtil"
    )

    workspace_uploader._update_upload_status_yaml_file(
        "CI", 12345, "214", "12345_58_1", assembly_tuple
    )
    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 12345, "214", assembly_dir,
    )

    expected_data = {
        "CI": {12345: {"214": {"assembly_upa": "12345_58_1",
                               "assembly_filename": assembly_name,
                               "genome_upa": None,
                               "genome_filename": None}}}}

    assert uploaded
    assert expected_data == data

    with pytest.raises(ValueError, match=f"already exists in workspace"):
        workspace_uploader._update_upload_status_yaml_file(
            "CI", 12345, "214", "12345_58_1", assembly_tuple
        )


def test_fetch_objects_to_upload(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs
    collection_source_dir = params.collection_source_dir

    count, wait_to_upload_assemblies = workspace_uploader._fetch_objects_to_upload(
        "CI",
        12345,
        "214",
        collection_source_dir,
        workspace_uploader._UPLOAD_FILE_EXT,
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
        assembly_tuple = workspace_uploader._WSObjTuple(
            assembly_name, assembly_dir, "/path/to/file/in/AssembilyUtil"
        )
        workspace_uploader._update_upload_status_yaml_file(
            "CI", 12345, "214", upa, assembly_tuple
        )

    (
        new_count,
        new_wait_to_upload_assemblies,
    ) = workspace_uploader._fetch_objects_to_upload(
        "CI",
        12345,
        "214",
        collection_source_dir,
        workspace_uploader._UPLOAD_FILE_EXT,
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
    assembly_tuple = workspace_uploader._WSObjTuple(
        assembly_name, host_assembly_dir, "/path/to/file/in/AssembilyUtil"
    )

    workspace_uploader._post_process(
        "CI",
        88888,
        "214",
        assembly_tuple,
        upload_dir,
        output_dir,
        "12345_58_1",
    )

    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 88888, "214", host_assembly_dir
    )
    expected_data = {
        "CI": {88888: {"214": {"assembly_upa": "12345_58_1",
                               "assembly_filename": assembly_name,
                               "genome_upa": None,
                               "genome_filename": None}}}}

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
    params = setup_and_teardown
    assembly_name = ASSEMBLY_NAMES[0]
    host_assembly_dir = params.assembly_dirs[0]

    asu = create_autospec(AssemblyUtil, spec_set=True, instance=True)
    asu.save_assemblies_from_fastas.return_value = {"results":[{"upa": "12345/58/1"}]}
    assembly_tuple = workspace_uploader._WSObjTuple(
        assembly_name, host_assembly_dir, "/path/to/file/in/AssembilyUtil"
    )
    upas = workspace_uploader._upload_assemblies_to_workspace(
        asu, 12345, "214", [assembly_tuple]
    )
    assert upas == tuple(["12345_58_1"])
    asu.save_assemblies_from_fastas.assert_called_once_with(
        {
            "workspace_id": 12345,
            "inputs": [
                {
                    "file": assembly_tuple.container_internal_file_dir,
                    "assembly_name": assembly_tuple.obj_name,
                    "object_metadata": {"load_id": "214"},
                }
            ]
        }
    )


def test_generator(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs
    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }
    assemblyTuple_list = list(workspace_uploader._gen(wait_to_upload_assemblies, 1))
    expected_assemblyTuple_list = [
        [
            workspace_uploader._WSObjTuple(
                "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
                assembly_dirs[0],
                "/kb/module/work/tmp/GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
            )
        ],
        [
            workspace_uploader._WSObjTuple(
                "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
                assembly_dirs[1],
                "/kb/module/work/tmp/GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
            )
        ],
    ]
    assert assemblyTuple_list == expected_assemblyTuple_list


def test_upload_assembly_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    src_files = params.target_files
    assembly_dirs = params.assembly_dirs
    upload_dir = Path(params.tmp_dir) / "upload_dir"
    upload_dir.mkdir()
    output_dir = Path(params.tmp_dir) / "output_dir"
    output_dir.mkdir()

    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }

    # ws.get_object_info3() is unused in this test case
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    asu = create_autospec(AssemblyUtil, spec_set=True, instance=True)
    asu.save_assemblies_from_fastas.return_value = {
        "results": [
            {"upa": "12345/58/1"},
            {"upa": "12345/60/1"}
        ]
    }

    uploaded_count = workspace_uploader._upload_assembly_files_in_parallel(
        asu,
        ws,
        "CI",
        12345,
        "214",
        upload_dir,
        wait_to_upload_assemblies,
        2,
        output_dir,
    )

    assert uploaded_count == 2

    # assert that no interactions occurred with ws
    ws.get_object_info3.assert_not_called()

    # assert that asu was called correctly
    asu.save_assemblies_from_fastas.assert_called_once_with(
        {
            "workspace_id": 12345,
            "inputs": [
                {
                    "file": "/kb/module/work/tmp/GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "assembly_name": "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "object_metadata": {"load_id": "214"},
                },
                {
                    "file": "/kb/module/work/tmp/GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "assembly_name": "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "object_metadata": {"load_id": "214"},
                }
            ]
        }
    )

    # check softlink for post_process
    assert os.readlink(os.path.join(upload_dir, "12345_58_1")) == os.path.join(
        output_dir, "12345_58_1"
    )
    assert os.readlink(os.path.join(upload_dir, "12345_60_1")) == os.path.join(
        output_dir, "12345_60_1"
    )

    # check hardlink for post_process
    assert os.path.samefile(
        src_files[0],
        os.path.join(output_dir, "12345_58_1", "12345_58_1.fna.gz")
    )

    assert os.path.samefile(
        src_files[1],
        os.path.join(output_dir, "12345_60_1", "12345_60_1.fna.gz")
    )


def test_fail_upload_assembly_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs
    upload_dir = Path(params.tmp_dir) / "upload_dir"
    upload_dir.mkdir()
    output_dir = Path(params.tmp_dir) / "output_dir"
    output_dir.mkdir()

    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }

    ws = create_autospec(Workspace, spec_set=True, instance=True)
    asu = create_autospec(AssemblyUtil, spec_set=True, instance=True)
    asu.save_assemblies_from_fastas.side_effect = Exception("Illegal character in object name")
    ws.get_object_info3.return_value = {
        'infos': [None, None], 'paths': [None, None]
    }

    uploaded_count = workspace_uploader._upload_assembly_files_in_parallel(
        asu,
        ws,
        "CI",
        12345,
        "214",
        upload_dir,
        wait_to_upload_assemblies,
        2,
        output_dir,
    )

    assert uploaded_count == 0

    # assert that asu was called correctly
    asu.save_assemblies_from_fastas.assert_called_once_with(
        {
            "workspace_id": 12345,
            "inputs": [
                {
                    "file": "/kb/module/work/tmp/GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "assembly_name": "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "object_metadata": {"load_id": "214"},
                },
                {
                    "file": "/kb/module/work/tmp/GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "assembly_name": "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
                    "object_metadata": {"load_id": "214"},
                }
            ]
        }
    )

    # assert that ws was called correctly
    ws.get_object_info3.assert_called_once_with(
        {
            "objects": [
                {"wsid": 12345, "name": "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz"},
                {"wsid": 12345, "name": "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz"}
            ],
            "ignoreErrors": 1,
            "includeMetadata": 1
        }
    )


def test_fail_query_workspace_with_load_id_mass(setup_and_teardown):
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    with pytest.raises(
        Exception, match="The effective max batch size must be <= 10000"
    ):
        workspace_uploader._query_workspace_with_load_id_mass(
            ws,
            12345,
            "214",
            [str(num) for num in range(100001)],
            10001,
        )

    # assert that no interactions occurred with ws
    ws.get_object_info3.assert_not_called()


def test_query_workspace_with_load_id_mass(setup_and_teardown):
    # happy test
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    ws.get_object_info3.return_value = {
        'infos': [
                    [
                        1086,
                        'GCF_000980105.1_gtlEnvA5udCFS_genomic.fna.gz',
                        'KBaseGenomeAnnotations.Assembly-6.3',
                        '2024-01-18T23:12:44+0000',
                        18,
                        'sijiex',
                        69046,
                        'sijiex:narrative_1688077625427',
                        'aaa726d2b976e27e729ac288812e81f6',
                        71823,
                        {
                            'GC content': '0.41571',
                            'Size': '4079204',
                            'N Contigs': '260',
                            'MD5': '8aa6b1244e18c4f93bb3307902bd3a4d',
                            "load_id": "998"
                        }
                    ],
                    [
                        1068,
                        'GCF_000979375.1_gtlEnvA5udCFS_genomic.fna.gz',
                        'KBaseGenomeAnnotations.Assembly-6.3',
                        '2024-01-18T23:12:35+0000',
                        18,
                        'sijiex',
                        69046,
                        'sijiex:narrative_1688077625427',
                        '866033827fd54569c953e8b3dd58d0aa',
                        38242,
                        {
                            'GC content': '0.41526',
                            'Size': '4092300',
                            'N Contigs': '136',
                            'MD5': '1e007bad0811a6d6e09a882d3bf802ab',
                            "load_id": "998"
                        }
                    ],
                    None],
        'paths': [['69046/1086/18'], ['69046/1068/18'], None]
    }

    obj_names, obj_upas = workspace_uploader._query_workspace_with_load_id_mass(
        ws,
        69046,
        "998",
        [
            "GCF_000980105.1_gtlEnvA5udCFS_genomic.fna.gz",
            "GCF_000979375.1_gtlEnvA5udCFS_genomic.fna.gz",
            "aloha",
        ]
    )
    assert obj_names == [
        "GCF_000980105.1_gtlEnvA5udCFS_genomic.fna.gz",
        "GCF_000979375.1_gtlEnvA5udCFS_genomic.fna.gz",
    ]
    assert obj_upas == ["69046_1086_18", "69046_1068_18"]

    # assert that ws was called correctly
    ws.get_object_info3.assert_called_once_with(
        {
            "objects": [
                {"wsid": 69046, "name": "GCF_000980105.1_gtlEnvA5udCFS_genomic.fna.gz"},
                {"wsid": 69046, "name": "GCF_000979375.1_gtlEnvA5udCFS_genomic.fna.gz"},
                {"wsid": 69046, "name": "aloha"}
            ],
            "ignoreErrors": 1,
            "includeMetadata": 1
        }
    )
