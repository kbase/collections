import json
import os
import shutil
import uuid
from pathlib import Path
from typing import NamedTuple
from unittest.mock import Mock, create_autospec, call, patch

import pytest

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.GenomeFileUtilClient import GenomeFileUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_helper
from src.loaders.workspace_uploader import workspace_uploader
from src.loaders.workspace_uploader.upload_result import UploadResult

ASSEMBLY_DIR_NAMES = ["GCF_000979855.1", "GCF_000979175.1"]
ASSEMBLY_NAMES = [
    "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
    "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
]
GEMOME_NAMES = [
    "GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz",
    "GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz",
]


class Params(NamedTuple):
    tmp_dir: str
    sourcedata_dir: str
    collection_source_dir: str
    assembly_dirs: list[str]
    target_files: list[str]
    genbank_files: list[str]


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

    assembly_dirs, target_files, genbank_files = list(), list(), list()
    for assembly_dir_name, assembly_name, genome_name in zip(ASSEMBLY_DIR_NAMES, ASSEMBLY_NAMES, GEMOME_NAMES):
        target_dir_path = sourcedata_dir.joinpath(assembly_dir_name)
        target_dir_path.mkdir()
        target_file_path = target_dir_path.joinpath(assembly_name)
        target_file_path.touch()
        genbank_file_path = target_dir_path.joinpath(genome_name)
        genbank_file_path.touch()
        new_dir_path = collection_source_dir.joinpath(assembly_dir_name)
        os.symlink(
            target_dir_path.resolve(), new_dir_path.resolve(), target_is_directory=True
        )
        assembly_dirs.append(str(new_dir_path))
        target_files.append(str(target_file_path))
        genbank_files.append(str(genbank_file_path))

    yield Params(
        tmp_dir, sourcedata_dir, collection_source_dir, assembly_dirs, target_files, genbank_files
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
    assembly_tuple = workspace_uploader.WSObjTuple(
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
        workspace_uploader._UPLOAD_ASSEMBLY_FILE_EXT,
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
        assembly_tuple = workspace_uploader.WSObjTuple(
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
        workspace_uploader._UPLOAD_ASSEMBLY_FILE_EXT,
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


def test_post_process_with_assembly_only(setup_and_teardown):
    params = setup_and_teardown

    host_assembly_dir = params.assembly_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]

    assembly_tuple = workspace_uploader.WSObjTuple(
        assembly_name, host_assembly_dir, "/path/to/file/in/AssembilyUtil"
    )

    workspace_uploader._post_process(
        "CI",
        88888,
        "214",
        params.collection_source_dir,
        params.sourcedata_dir,
        assembly_tuple,
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

    assert uploaded
    assert expected_data == data


def test_post_process_with_genome(setup_and_teardown):
    # test with genome_tuple and genome_upa
    params = setup_and_teardown
    collections_source_dir = params.collection_source_dir
    source_dir = params.sourcedata_dir

    host_assembly_dir = params.assembly_dirs[1]
    assembly_name = ASSEMBLY_NAMES[1]
    src_file = params.target_files[1]
    assembly_tuple = workspace_uploader.WSObjTuple(
        assembly_name, host_assembly_dir, "/path/to/file/in/AssembilyUtil"
    )
    genome_name = GEMOME_NAMES[1]
    genome_tuple = workspace_uploader.WSObjTuple(
        genome_name, host_assembly_dir, "/path/to/file/in/GenomeUtil"
    )
    assembly_obj_info = [4, 'name', 'type', 'time', 75, 'user', 42, 'wsname', 'md5', 78, {'foo': 'bar'}]
    genome_obj_info = [7, 'name', 'type', 'time', 1, 'user', 42, 'wsname', 'md5', 78, {}]
    workspace_uploader._post_process(
        "CI",
        88888,
        "214",
        collections_source_dir,
        source_dir,
        assembly_tuple,
        "42_4_75",
        genome_tuple=genome_tuple,
        genome_upa="42_7_1",
        assembly_obj_info=assembly_obj_info,
        genome_obj_info=genome_obj_info
    )

    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 88888, "214", host_assembly_dir
    )

    expected_data = {
        'CI': {88888: {'214':
                           {'assembly_filename': assembly_name,
                            'assembly_upa': '42_4_75',
                            'genome_filename': genome_name,
                            'genome_upa': '42_7_1'}}}}

    assert uploaded
    assert expected_data == data

    assembly_dest_file = source_dir / "42_4_75" / "42_4_75.fna.gz"
    # check softlink
    assert os.readlink(os.path.join(collections_source_dir, "42_4_75")) == os.path.join(
        source_dir, "42_4_75"
    )
    # check hardlink
    assert os.path.samefile(src_file, assembly_dest_file)

    # check metadata file
    metadata_file = source_dir / "42_4_75" / "42_4_75.meta"
    with open(metadata_file, 'r') as file:
        data = json.load(file)

    expected_metadata = {
        'upa': '42/4/75',
        'name': 'name',
        'timestamp': 'time',
        'type': 'type',
        'genome_upa': '42/7/1',
        'assembly_object_info': assembly_obj_info,
        'genome_object_info': genome_obj_info
    }
    assert data == expected_metadata


def test_upload_assembly_to_workspace(setup_and_teardown):
    params = setup_and_teardown
    assembly_name = ASSEMBLY_NAMES[0]
    host_assembly_dir = params.assembly_dirs[0]

    asu = create_autospec(AssemblyUtil, spec_set=True, instance=True)
    asu.save_assemblies_from_fastas.return_value = {
        "results": [{"upa": "12345/1/58",
                     "object_info": [1, 'assembly_1', 'KBaseGenomeAnnotations.Assembly-6', 'time', 58, 'user', 12345, 'wsname', 'md5', 78, {'foo': 'bar'}]}]
    }
    assembly_tuple = workspace_uploader.WSObjTuple(
        assembly_name, host_assembly_dir, "/path/to/file/in/AssembilyUtil"
    )
    upload_results = workspace_uploader._upload_assemblies_to_workspace(
        asu, 12345, "214", [assembly_tuple]
    )
    assert [r.assembly_upa for r in upload_results] == ["12345_1_58"]
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


def test_upload_genome_to_workspace(setup_and_teardown):
    params = setup_and_teardown
    genome_coll_src_dir = Path(params.collection_source_dir) / "NewGenome"
    genome_coll_src_dir.mkdir(parents=True)
    job_dir = Path(params.tmp_dir) / "kb/module/work/tmp"
    job_dir.mkdir(parents=True)
    shutil.copy(params.target_files[0], job_dir)

    load_id = "214"
    genome_obj_info = [1, 'genome_1', 'KBaseGenomes.Genome-6', 'time', 58, 'user', 12345, 'wsname',
                       'md5', 78, {"load_id": load_id, "Assembly Object": "1/1/1"}]
    assembly_obj_info = [1, 'assembly_1', 'KBaseGenomeAnnotations.Assembly-6', 'time', 59, 'user', 12345, 'wsname',
                         'md5', 78, {'foo': 'bar'}]

    gfu = create_autospec(GenomeFileUtil, spec_set=True, instance=True)
    gfu.genbanks_to_genomes.return_value = {"results": [{"genome_ref": "12345/1/58",
                                                         "assembly_ref": "12345/1/59",
                                                         "assembly_path": job_dir / ASSEMBLY_NAMES[0],
                                                         "assembly_info": assembly_obj_info,
                                                         "genome_info": genome_obj_info}]}
    genome_tuple = workspace_uploader.WSObjTuple(
        "genome_name", genome_coll_src_dir, "/path/to/file/in/AssembilyUtil"
    )

    with patch.object(workspace_uploader, '_JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER', new=job_dir):
        upload_results = workspace_uploader._upload_genomes_to_workspace(
            gfu, 12345, "214", [genome_tuple], job_dir
        )

    expected_assembly_tuple = workspace_uploader.WSObjTuple(
        obj_name=ASSEMBLY_NAMES[0],
        obj_coll_src_dir=genome_coll_src_dir,
        container_internal_file_dir=job_dir / ASSEMBLY_NAMES[0])

    expected_upload_results = [UploadResult(
        assembly_obj_info=assembly_obj_info,
        assembly_tuple=expected_assembly_tuple,
        genome_obj_info=genome_obj_info,
        genome_tuple=genome_tuple)]

    assert upload_results == expected_upload_results

    gfu.genbanks_to_genomes.assert_called_once_with(
        {
            "workspace_id": 12345,
            "inputs": [
                {
                    "file": {"path": genome_tuple.container_internal_file_dir},
                    "genome_name": genome_tuple.obj_name,
                    "metadata": {"load_id": "214"},
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
            workspace_uploader.WSObjTuple(
                "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
                assembly_dirs[0],
                "/kb/module/work/tmp/GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
            )
        ],
        [
            workspace_uploader.WSObjTuple(
                "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
                assembly_dirs[1],
                "/kb/module/work/tmp/GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
            )
        ],
    ]
    assert assemblyTuple_list == expected_assemblyTuple_list


def test_upload_genome_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    collection_source_dir = params.collection_source_dir
    sourcedata_dir = params.sourcedata_dir
    src_files = params.target_files
    genbank_files = params.genbank_files
    genome_names = ["NewGenome1", "NewGenome2"]
    genome_collection_source_dirs = list()
    for genome_name, genbank_file in zip(genome_names, genbank_files):
        genome_source_data_dir = sourcedata_dir / genome_name
        genome_source_data_dir.mkdir(parents=True)
        shutil.copy(genbank_file, genome_source_data_dir)
        os.symlink(
            genome_source_data_dir.resolve(), collection_source_dir / genome_name, target_is_directory=True
        )
        genome_collection_source_dirs.append(collection_source_dir / genome_name)

    job_dir = Path(params.tmp_dir) / "kb/module/work/tmp"
    job_dir.mkdir(parents=True)
    # copy the source files to the job_dir
    for src_file in src_files:
        shutil.copy(src_file, job_dir)

    load_id = "214"

    assembly_obj_infos = [
        [1, 'assembly_1', 'KBaseGenomeAnnotations.Assembly-6', 'time', 1, 'user', 42, 'wsname', 'md5', 78, {'foo': 'bar'}],
        [2, 'assembly_2', 'KBaseGenomeAnnotations.Assembly-10', 'time', 1, 'user', 42, 'wsname', 'md5', 78, {}]
    ]

    genome_obj_infos = [
        [3, 'genome_1', 'KBaseGenomes.Genome-6', 'time', 1, 'user', 42, 'wsname', 'md5', 78,
         {"load_id": load_id, "Assembly Object": "1/1/1"}],
        [4, 'genome_2', 'KBaseGenomes.Genome-9.3', 'time', 1, 'user', 42, 'wsname', 'md5', 78,
         {"load_id": load_id, "Assembly Object": "2/2"}]
    ]

    wait_to_upload_genomes = {
        genome_name: genome_dir
        for genome_name, genome_dir in zip(GEMOME_NAMES, genome_collection_source_dirs)
    }

    # ws.get_object_info3() is unused in this test case
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    gfu = create_autospec(GenomeFileUtil, spec_set=True, instance=True)
    genbanks_to_genomes_results = {
        "results": [{"genome_ref": "42/3/1",
                     "assembly_ref": "42/1/1",
                     "assembly_path": job_dir / ASSEMBLY_NAMES[0],
                     "assembly_info": assembly_obj_infos[0],
                     "genome_info": genome_obj_infos[0]},
                    {"genome_ref": "42/4/1",
                     "assembly_ref": "42/2/1",
                     "assembly_path": job_dir / ASSEMBLY_NAMES[1],
                     "assembly_info": assembly_obj_infos[1],
                     "genome_info": genome_obj_infos[1]}]
    }
    gfu.genbanks_to_genomes.return_value = genbanks_to_genomes_results

    with patch.object(workspace_uploader, '_JOB_DIR_IN_ASSEMBLYUTIL_CONTAINER', new=job_dir):
        uploaded_count = workspace_uploader._upload_objects_in_parallel(
            ws=ws,
            upload_env_key="CI",
            workspace_id=42,
            load_id="214",
            collections_source_dir=collection_source_dir,
            wait_to_upload_objs=wait_to_upload_genomes,
            batch_size=2,
            source_data_dir=sourcedata_dir,
            asu_client=Mock(),
            gfu_client=gfu,
            job_data_dir=job_dir,
            upload_assembly_only=False,
        )

    assert uploaded_count == 2

    # assert that no interactions occurred with ws
    ws.get_object_info3.assert_not_called()

    # assert that asu was called correctly
    gfu.genbanks_to_genomes.assert_called_once_with(
        {
            "workspace_id": 42,
            "inputs": [
                {
                    "file": {"path": f"{job_dir}/GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz"},
                    "genome_name": "GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz",
                    "metadata": {"load_id": "214"},
                },
                {
                    "file": {"path": f"{job_dir}/GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz"},
                    "genome_name": "GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz",
                    "metadata": {"load_id": "214"},
                }
            ]
        }
    )

    # check softlink for post_process
    assert os.readlink(os.path.join(collection_source_dir, "42_1_1")) == os.path.join(
        sourcedata_dir, "42_1_1"
    )
    assert os.readlink(os.path.join(collection_source_dir, "42_2_1")) == os.path.join(
        sourcedata_dir, "42_2_1"
    )

    # check hardlink for post_process
    assert os.path.samefile(
        genome_collection_source_dirs[0] / ASSEMBLY_NAMES[0],
        os.path.join(sourcedata_dir, "42_1_1", "42_1_1.fna.gz")
    )

    assert os.path.samefile(
        genome_collection_source_dirs[1] / ASSEMBLY_NAMES[1],
        os.path.join(sourcedata_dir, "42_2_1", "42_2_1.fna.gz")
    )

    # check metadata file
    metadata_file = sourcedata_dir / "42_1_1" / "42_1_1.meta"
    with open(metadata_file, 'r') as file:
        data = json.load(file)

    expected_metadata = {
        'upa': '42/1/1',
        'name': 'assembly_1',
        'timestamp': 'time',
        'type': 'KBaseGenomeAnnotations.Assembly-6',
        'genome_upa': '42/3/1',
        'assembly_object_info': assembly_obj_infos[0],
        'genome_object_info': genome_obj_infos[0]
    }
    assert data == expected_metadata

    metadata_file = sourcedata_dir / "42_2_1" / "42_2_1.meta"
    with open(metadata_file, 'r') as file:
        data = json.load(file)

    expected_metadata = {
        'upa': '42/2/1',
        'name': 'assembly_2',
        'timestamp': 'time',
        'type': 'KBaseGenomeAnnotations.Assembly-10',
        'genome_upa': '42/4/1',
        'assembly_object_info': assembly_obj_infos[1],
        'genome_object_info': genome_obj_infos[1]
    }
    assert data == expected_metadata


def test_upload_assembly_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs

    wait_to_upload_assemblies = {
        assembly_name: assembly_dir
        for assembly_name, assembly_dir in zip(ASSEMBLY_NAMES, assembly_dirs)
    }

    # ws.get_object_info3() is unused in this test case
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    asu = create_autospec(AssemblyUtil, spec_set=True, instance=True)
    asu.save_assemblies_from_fastas.return_value = {
        "results": [
            {"upa": "12345/58/1",
             "object_info": [58, 'assembly_1', 'KBaseGenomeAnnotations.Assembly-6', 'time', 1, 'user', 12345, 'wsname', 'md5', 78, {'foo': 'bar'}]},
            {"upa": "12345/60/1",
             "object_info": [60, 'assembly_2', 'KBaseGenomeAnnotations.Assembly-10', 'time', 1, 'user', 12345, 'wsname', 'md5', 78, {}]}
        ]
    }

    uploaded_count = workspace_uploader._upload_objects_in_parallel(
        ws,
        "CI",
        12345,
        "214",
        params.collection_source_dir,
        wait_to_upload_assemblies,
        2,
        params.sourcedata_dir,
        asu_client=asu,
        gfu_client=Mock(),
        job_data_dir='path/to/job_data_dir'
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


def test_fail_upload_assembly_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    assembly_dirs = params.assembly_dirs

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

    uploaded_count = workspace_uploader._upload_objects_in_parallel(
        ws=ws,
        upload_env_key="CI",
        workspace_id=12345,
        load_id="214",
        collections_source_dir=params.sourcedata_dir,
        wait_to_upload_objs=wait_to_upload_assemblies,
        batch_size=2,
        source_data_dir=params.sourcedata_dir,
        asu_client=asu,
        gfu_client=Mock(),
        job_data_dir='path/to/job_data_dir'
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


def test_query_workspace_with_load_id_mass_assembly(setup_and_teardown):
    # test with assembly objects only
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    mock_object_info = {
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
    ws.get_object_info3.return_value = mock_object_info

    assembly_objs_info, genome_objs_info = workspace_uploader._query_workspace_with_load_id_mass(
        ws,
        69046,
        "998",
        [
            "GCF_000980105.1_gtlEnvA5udCFS_genomic.fna.gz",
            "GCF_000979375.1_gtlEnvA5udCFS_genomic.fna.gz",
            "aloha",
        ]
    )

    assert assembly_objs_info == mock_object_info['infos'][:2]
    assert genome_objs_info == []

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


def test_query_workspace_with_load_id_mass_genome(setup_and_teardown):
    # Test case 1: Valid scenario - test with genome objects and assembly_objs_only is set to False
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    load_id = "998"
    assembly_objs_response = {
        "infos": [
            [1, 'assembly_1', 'KBaseGenomeAnnotations.Assembly-6', 'time', 75, 'user', 42, 'wsname', 'md5', 78, {'foo': 'bar'}],
            [2, 'assembly_2', 'KBaseGenomeAnnotations.Assembly-10', 'time', 75, 'user', 42, 'wsname', 'md5', 78, {}]
        ]
    }

    genome_objs_response = {
        "infos": [
            [1, 'genome_1', 'KBaseGenomes.Genome-6', 'time', 75, 'user', 42, 'wsname', 'md5', 78,
             {"load_id": load_id, "Assembly Object": "1/1/1"}],
            [4, 'genome_2', 'KBaseGenomes.Genome-9.3', 'time', 75, 'user', 42, 'wsname', 'md5', 78,
             {"load_id": load_id, "Assembly Object": "2/2/2"}]
        ]
    }

    ws.get_object_info3.side_effect = [genome_objs_response, assembly_objs_response]

    assembly_objs_info, genome_objs_info = workspace_uploader._query_workspace_with_load_id_mass(
        ws,
        69046,
        load_id,
        ["genome_1", "genome_2",],
        assembly_objs_only=False
    )
    assert assembly_objs_info == assembly_objs_response['infos']
    assert genome_objs_info == genome_objs_response['infos']

    # Assert expected calls to ws.get_object_info3
    expected_calls = [
        call({"objects": [{"wsid": 69046, "name": "genome_1"},
                          {"wsid": 69046, "name": "genome_2"}], "ignoreErrors": 1, "includeMetadata": 1}),
        call({"objects": [{"ref": "1/1/1"}, {"ref": "2/2/2"}], "includeMetadata": 1}),
    ]
    ws.get_object_info3.assert_has_calls(expected_calls, any_order=False)
    assert ws.get_object_info3.call_count == 2


def test_query_workspace_with_load_id_mass_genome_fail(setup_and_teardown):

    ws = create_autospec(Workspace, spec_set=True, instance=True)
    load_id = "998"

    # Invalid scenario - genome object does not have an 'Assembly Object' field in its metadata
    genome_objs_response = {
        "infos": [
            [1, 'genome_1', 'KBaseGenomes.Genome-6', 'time', 75, 'user', 42, 'wsname', 'md5', 78,
             {"load_id": load_id}]
        ]
    }

    ws.get_object_info3.side_effect = [genome_objs_response]
    with pytest.raises(ValueError) as excinfo:
        workspace_uploader._query_workspace_with_load_id_mass(
            ws,
            69046,
            load_id,
            ["genome_1"],
            assembly_objs_only=False
        )
    assert "Genome object 42/1/75 does not have an assembly object linked to it" == str(excinfo.value)


def test_check_obj_type():
    workspace_id = 1
    load_id = "12345"
    obj_infos = [
        [1, 'abc-123', 'TypeA', 'time', 75, 'user', 42, 'wsname', 'md5', 78, {'foo': 'bar'}],
        [2, 'def-456', 'TypeB', 'time', 75, 'user', 42, 'wsname', 'md5', 78, {}]
    ]
    expected_obj_types = {'TypeA', 'TypeB'}

    workspace_uploader._check_obj_type(workspace_id, load_id, obj_infos, expected_obj_types)


def test_check_obj_type_fail():

    workspace_id = 1
    load_id = "12345"
    obj_infos = [
        [1, 'abc-123', 'TypeA', 'time', 75, 'user', 42, 'wsname', 'md5', 78, {'foo': 'bar'}],
        [2, 'def-456', 'TypeB', 'time', 75, 'user', 42, 'wsname', 'md5', 78, {}]
    ]

    expected_obj_types = {'TypeA', 'TypeC'}
    with pytest.raises(ValueError) as excinfo:
        workspace_uploader._check_obj_type(workspace_id, load_id, obj_infos, expected_obj_types)

    assert "Only expecting ['TypeA', 'TypeC'] objects" in str(excinfo.value)

