import json
import os
import shutil
import uuid
from pathlib import Path
from typing import NamedTuple
from unittest.mock import Mock, create_autospec, call, patch

import pytest

from src.clients.GenomeFileUtilClient import GenomeFileUtil
from src.clients.workspaceClient import Workspace
from src.common.common_helper import obj_info_to_upa
from src.loaders.common import loader_helper
from src.loaders.workspace_uploader import workspace_uploader
from src.loaders.workspace_uploader.upload_result import UploadResult

GENOME_DIR_NAMES = ["GCF_000979855.1", "GCF_000979175.1"]
ASSEMBLY_NAMES = [
    "GCF_000979855.1_gtlEnvA5udCFS_genomic.fna.gz",
    "GCF_000979175.1_gtlEnvA5udCFS_genomic.fna.gz",
]
GEMOME_NAMES = [
    "GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz",
    "GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz",
]
ASSEMBLY_OBJ_INFOS = [
    [
        60,
        ASSEMBLY_NAMES[0],
        "KBaseGenomeAnnotations.Assembly-6.3",
        "2024-03-01T18:47:59+0000",
        1,
        "tgu2",
        72231,
        "tgu2:narrative_1706737132837",
        "11d4f238f1fc4ce420322c999cb7e879",
        53669,
        {
            "GC content": "0.41622",
            "Size": "4078931",
            "N Contigs": "185",
            "MD5": "a26f200923f8c860f86a8d728055fd02"
        }
    ],
    [
        7,
        ASSEMBLY_NAMES[1],
        "KBaseGenomeAnnotations.Assembly-6.3",
        "2024-03-01T18:18:45+0000",
        2,
        "tgu2",
        72231,
        "tgu2:narrative_1706737132837",
        "957989f1b48d713ee95dc388807ea54f",
        914,
        {
            "GC content": "0.5207",
            "Size": "10000",
            "N Contigs": "1",
            "MD5": "a049c3e96aabd0821b83715bf0ca4250"
        }
    ]
]
GENOME_OBJ_INFOS = [
    [
        61,
        GEMOME_NAMES[0],
        "KBaseGenomes.Genome-17.2",
        "2024-03-01T18:49:15+0000",
        1,
        "tgu2",
        72231,
        "tgu2:narrative_1706737132837",
        "6b981d906e956afbb93c678ce12839cf",
        12205456,
        {
            "load_id": "1",
            "Taxonomy": "Unconfirmed Organism",
            "Size": "4078931",
            "Source": "Genbank",
            "Name": "Methanosarcina mazei",
            "GC content": "0.41622",
            "Genetic code": "11",
            "Suspect Genome": "1",
            "Number of Genome Level Warnings": "16",
            "Source ID": "NZ_JJPE01000015",
            "Number of Protein Encoding Genes": "3475",
            "Assembly Object": "72231/60/1",
            "Number contigs": "185",
            "Domain": "Unknown",
            "Number of CDS": "3475",
            "MD5": "a26f200923f8c860f86a8d728055fd02"
        }
    ],
    [
        8,
        GEMOME_NAMES[1],
        "KBaseGenomes.Genome-17.2",
        "2024-03-01T18:19:14+0000",
        2,
        "tgu2",
        72231,
        "tgu2:narrative_1706737132837",
        "2f8ccd9828c55f6d59d33315c7ceb60e",
        15742,
        {
            "Taxonomy": "Unconfirmed Organism",
            "Size": "10000",
            "Source": "RefSeq",
            "Name": "Escherichia coli str. K-12 substr. MG1655",
            "GC content": "0.5207",
            "Genetic code": "11",
            "Suspect Genome": "1",
            "Number of Genome Level Warnings": "2",
            "Source ID": "NC_000913",
            "Number of Protein Encoding Genes": "3",
            "Assembly Object": "72231/7/2",
            "Number contigs": "1",
            "Domain": "Unknown",
            "Number of CDS": "3",
            "Genome Type": "draft isolate",
            "MD5": "a049c3e96aabd0821b83715bf0ca4250"
        }
    ]
]


class Params(NamedTuple):
    tmp_dir: str
    sourcedata_dir: str
    collection_source_dir: str
    genome_dirs: list[str]
    assembly_files: list[str]
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

    genome_dirs, assembly_files, genbank_files = list(), list(), list()
    for genome_dir_name, assembly_name, genome_name in zip(GENOME_DIR_NAMES, ASSEMBLY_NAMES, GEMOME_NAMES):
        target_dir_path = sourcedata_dir.joinpath(genome_dir_name)
        target_dir_path.mkdir()
        assembly_file_path = target_dir_path.joinpath(assembly_name)
        assembly_file_path.touch()
        genbank_file_path = target_dir_path.joinpath(genome_name)
        genbank_file_path.touch()
        new_dir_path = collection_source_dir.joinpath(genome_dir_name)
        os.symlink(
            target_dir_path.resolve(), new_dir_path.resolve(), target_is_directory=True
        )
        genome_dirs.append(str(new_dir_path))
        assembly_files.append(str(assembly_file_path))
        genbank_files.append(str(genbank_file_path))

    yield Params(
        tmp_dir, sourcedata_dir, collection_source_dir, genome_dirs, assembly_files, genbank_files
    )

    shutil.rmtree(tmp_dir, ignore_errors=True)


def test_get_yaml_file_path(setup_and_teardown):
    params = setup_and_teardown
    genome_dir = params.genome_dirs[0]
    yaml_path = workspace_uploader._get_yaml_file_path(genome_dir)

    expected_yaml_path = os.path.join(genome_dir, workspace_uploader._UPLOADED_YAML)
    assert expected_yaml_path == yaml_path
    assert os.path.exists(yaml_path)


def test_get_source_file(setup_and_teardown):
    params = setup_and_teardown
    genome_dir, genome_name = params.genome_dirs[0], GEMOME_NAMES[0]
    file_path = workspace_uploader._get_source_file(genome_dir, genome_name)

    expected_target_file_path = os.path.abspath(params.genbank_files[0])
    assert expected_target_file_path == file_path


def test_read_upload_status_yaml_file(setup_and_teardown):
    params = setup_and_teardown
    genome_dir = params.genome_dirs[0]

    # test empty yaml file in genome_dir
    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 12345, "214", genome_dir
    )

    expected_data = {
        "CI": {12345: {}}}

    assert not uploaded
    assert expected_data == data


def test_update_upload_status_yaml_file(setup_and_teardown):
    params = setup_and_teardown
    genome_dir = params.genome_dirs[0]
    assembly_name = ASSEMBLY_NAMES[0]
    assembly_tuple = workspace_uploader.WSObjTuple(
        assembly_name, genome_dir, "/path/to/file/in/AssembilyUtil"
    )

    genome_name = GEMOME_NAMES[0]
    genome_tuple = workspace_uploader.WSObjTuple(
        genome_name, genome_dir, "/path/to/file/in/GenomeFileUtil"
    )

    upload_result = UploadResult(assembly_tuple=assembly_tuple,
                                 assembly_obj_info=ASSEMBLY_OBJ_INFOS[0],
                                 genome_tuple=genome_tuple,
                                 genome_obj_info=GENOME_OBJ_INFOS[0])

    workspace_uploader._update_upload_status_yaml_file(
        "CI", 12345, "214", upload_result
    )
    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 12345, "214", genome_dir,
    )

    expected_data = {
        "CI": {12345: {"214": {"assembly_upa": "72231_60_1",
                               "assembly_filename": assembly_name,
                               "genome_upa": "72231_61_1",
                               "genome_filename": genome_name}}}}

    assert uploaded
    assert expected_data == data

    with pytest.raises(ValueError, match=f"already exists in workspace"):
        workspace_uploader._update_upload_status_yaml_file(
            "CI", 12345, "214", upload_result
        )


def test_fetch_objects_to_upload(setup_and_teardown):
    params = setup_and_teardown
    genome_dirs = params.genome_dirs
    collection_source_dir = params.collection_source_dir

    count, wait_to_upload_genomes = workspace_uploader._fetch_objects_to_upload(
        "CI",
        12345,
        "214",
        collection_source_dir,
        workspace_uploader._UPLOAD_GENOME_FILE_EXT,
    )

    expected_count = len(GEMOME_NAMES)
    expected_wait_to_upload_genomes = {
        genome_name: genome_dir
        for genome_name, genome_dir in zip(GEMOME_NAMES, genome_dirs)
    }

    assert expected_count == count
    assert expected_wait_to_upload_genomes == wait_to_upload_genomes

    # let's assume these two genome files are uploaded successfully
    # Each UPLOADED_YAML file is also updated with the upa assigned from the workspace service
    # Both genome files will be skipped in the next fetch_assemblies_to_upload call
    for genome_name, assembly_name, genome_dir in zip(GEMOME_NAMES, ASSEMBLY_NAMES, genome_dirs):
        assembly_tuple = workspace_uploader.WSObjTuple(
            assembly_name, genome_dir, "/path/to/file/in/AssembilyUtil"
        )
        genome_tuple = workspace_uploader.WSObjTuple(
            genome_name, genome_dir, "/path/to/file/in/GenomeFileUtil"
        )
        upload_result = UploadResult(assembly_tuple=assembly_tuple,
                                     assembly_obj_info=ASSEMBLY_OBJ_INFOS[0],
                                     genome_tuple=genome_tuple,
                                     genome_obj_info=GENOME_OBJ_INFOS[0])
        workspace_uploader._update_upload_status_yaml_file(
            "CI", 12345, "214", upload_result
        )

    (
        new_count,
        new_wait_to_upload_genomes,
    ) = workspace_uploader._fetch_objects_to_upload(
        "CI",
        12345,
        "214",
        collection_source_dir,
        workspace_uploader._UPLOAD_GENOME_FILE_EXT,
    )

    assert expected_count == new_count
    assert {} == new_wait_to_upload_genomes


def test_prepare_skd_job_dir_to_upload(setup_and_teardown):
    params = setup_and_teardown
    genome_dirs = params.genome_dirs
    wait_to_upload_genomes = {
        genome_name: genome_dir
        for genome_name, genome_dir in zip(GEMOME_NAMES, genome_dirs)
    }

    conf = Mock()
    job_dir = loader_helper.make_job_dir(params.tmp_dir, "kbase")
    conf.job_data_dir = loader_helper.make_job_data_dir(job_dir)
    data_dir = workspace_uploader._prepare_skd_job_dir_to_upload(
        conf, wait_to_upload_genomes
    )

    assert sorted(os.listdir(data_dir)) == sorted(GEMOME_NAMES)
    for genome_name, src_file in zip(GEMOME_NAMES, params.genbank_files):
        assert os.path.samefile(src_file, os.path.join(data_dir, genome_name))


def test_post_process_with_genome(setup_and_teardown):
    # test with genome_tuple and genome_upa
    params = setup_and_teardown
    collections_source_dir = params.collection_source_dir
    source_dir = params.sourcedata_dir
    container_dir = Path(params.tmp_dir) / "kb/module/work/tmp"
    container_dir.mkdir(parents=True)
    genome_coll_src_dir = params.genome_dirs[1]

    assembly_obj_info = ASSEMBLY_OBJ_INFOS[1]
    genome_obj_info = GENOME_OBJ_INFOS[1]

    assembly_name = assembly_obj_info[1]
    genome_name = genome_obj_info[1]

    assembly_tuple = workspace_uploader.WSObjTuple(assembly_name, genome_coll_src_dir, container_dir / assembly_name)
    genome_tuple = workspace_uploader.WSObjTuple(genome_name, genome_coll_src_dir, container_dir / genome_name)

    assembly_upa = obj_info_to_upa(assembly_obj_info, underscore_sep=True)
    genome_upa = obj_info_to_upa(genome_obj_info, underscore_sep=True)

    workspace_uploader._post_process(
        "CI",
        88888,
        "214",
        collections_source_dir,
        source_dir,
        UploadResult(assembly_tuple=assembly_tuple,
                     genome_tuple=genome_tuple,
                     assembly_obj_info=assembly_obj_info,
                     genome_obj_info=genome_obj_info)
    )

    data, uploaded = workspace_uploader._read_upload_status_yaml_file(
        "CI", 88888, "214", genome_coll_src_dir
    )

    expected_data = {'CI': {88888: {'214': {'assembly_filename': assembly_name,
                                            'assembly_upa': assembly_upa,
                                            'genome_filename': genome_name,
                                            'genome_upa': genome_upa}}}}

    assert uploaded
    assert expected_data == data

    # check softlink
    assert os.readlink(os.path.join(collections_source_dir, assembly_upa)) == os.path.join(source_dir, assembly_upa)
    # check hardlink
    assembly_src_file = params.assembly_files[1]
    assembly_dest_file = source_dir / assembly_upa / f"{assembly_upa}.fna.gz"
    assert os.path.samefile(assembly_src_file, assembly_dest_file)
    # check metadata file
    metadata_file = source_dir / assembly_upa / f"{assembly_upa}.meta"
    with open(metadata_file, 'r') as file:
        data = json.load(file)

    expected_metadata = {
        'upa': obj_info_to_upa(assembly_obj_info),
        'name': assembly_obj_info[1],
        'timestamp': assembly_obj_info[3],
        'type': assembly_obj_info[2],
        'genome_upa': obj_info_to_upa(genome_obj_info),
        'assembly_object_info': assembly_obj_info,
        'genome_object_info': genome_obj_info
    }
    assert data == expected_metadata


def test_upload_genome_to_workspace(setup_and_teardown):
    params = setup_and_teardown
    genome_name = GEMOME_NAMES[0]
    assembly_name = ASSEMBLY_NAMES[0]
    genome_coll_src_dir = Path(params.collection_source_dir) / genome_name
    genome_coll_src_dir.mkdir(parents=True)
    container_dir = Path(params.tmp_dir) / "kb/module/work/tmp"
    container_dir.mkdir(parents=True)
    shutil.copy(params.assembly_files[0], container_dir)

    genome_container_file = container_dir / genome_name  # the existence of this file is not checked in the test

    genome_obj_info = GENOME_OBJ_INFOS[0]
    assembly_obj_info = ASSEMBLY_OBJ_INFOS[0]

    gfu = create_autospec(GenomeFileUtil, spec_set=True, instance=True)
    gfu.genbanks_to_genomes.return_value = {"results": [{"genome_ref": "72231/61/1",
                                                         "assembly_ref": "72231/60/1",
                                                         "assembly_path": container_dir / assembly_name,
                                                         "assembly_info": assembly_obj_info,
                                                         "genome_info": genome_obj_info}]}
    genome_tuple = workspace_uploader.WSObjTuple(
        genome_name, genome_coll_src_dir, genome_container_file
    )

    with patch.object(workspace_uploader, '_JOB_DIR_IN_CONTAINER', new=container_dir):
        upload_results = workspace_uploader._upload_genomes_to_workspace(
            gfu, 12345, "214", [genome_tuple], container_dir
        )

    expected_assembly_tuple = workspace_uploader.WSObjTuple(
        obj_name=assembly_name,
        obj_coll_src_dir=genome_coll_src_dir,
        container_internal_file=container_dir / assembly_name)

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
                    "file": {"path": genome_container_file},
                    "genome_name": genome_name,
                    "metadata": {"load_id": "214"},
                }
            ]
        }
    )

    # check hardlink for associated FASTA file
    assert os.path.samefile(
        genome_coll_src_dir / assembly_name,
        container_dir / assembly_name
    )


def test_generator(setup_and_teardown):
    params = setup_and_teardown
    genome_dirs = params.genome_dirs
    wait_to_upload_genomes = {
        genome_name: genome_dir
        for genome_name, genome_dir in zip(GEMOME_NAMES, genome_dirs)
    }
    genomeTuple_list = list(workspace_uploader._gen(wait_to_upload_genomes, 1))
    expected_genomeTuple_list = [
        [
            workspace_uploader.WSObjTuple(
                "GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz",
                genome_dirs[0],
                "/kb/module/work/tmp/GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz",
            )
        ],
        [
            workspace_uploader.WSObjTuple(
                "GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz",
                genome_dirs[1],
                "/kb/module/work/tmp/GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz",
            )
        ],
    ]
    assert genomeTuple_list == expected_genomeTuple_list


def test_upload_genome_files_in_parallel(setup_and_teardown):
    params = setup_and_teardown
    collection_source_dir = params.collection_source_dir
    sourcedata_dir = params.sourcedata_dir
    # Assembly files are normally produced by the GFU.
    # This test fakes that process by placing them in the container job directory in the setup below
    assembly_files = params.assembly_files
    genbank_files = params.genbank_files
    genome_ids = ["GCF_000979115.1", "GCF_000979555.1"]
    genome_collection_source_dirs = list()
    for genome_id, genbank_file in zip(genome_ids, genbank_files):
        genome_source_data_dir = sourcedata_dir / genome_id
        genome_source_data_dir.mkdir(parents=True)
        shutil.copy(genbank_file, genome_source_data_dir)
        os.symlink(
            genome_source_data_dir.resolve(), collection_source_dir / genome_id, target_is_directory=True
        )
        genome_collection_source_dirs.append(collection_source_dir / genome_id)

    container_dir = Path(params.tmp_dir) / "kb/module/work/tmp"
    container_dir.mkdir(parents=True)
    # copy the assembly files to the container_dir
    for assembly_file in assembly_files:
        shutil.copy(assembly_file, container_dir)

    assembly_refs = [obj_info_to_upa(info) for info in ASSEMBLY_OBJ_INFOS]
    genome_refs = [obj_info_to_upa(info) for info in GENOME_OBJ_INFOS]

    wait_to_upload_genomes = {
        genome_name: genome_dir
        for genome_name, genome_dir in zip(GEMOME_NAMES, genome_collection_source_dirs)
    }

    # ws.get_object_info3() is unused in this test case
    ws = create_autospec(Workspace, spec_set=True, instance=True)
    gfu = create_autospec(GenomeFileUtil, spec_set=True, instance=True)
    genbanks_to_genomes_results = {
        "results": [{"genome_ref": genome_refs[0],
                     "assembly_ref": assembly_refs[0],
                     "assembly_path": container_dir / ASSEMBLY_NAMES[0],
                     "assembly_info": ASSEMBLY_OBJ_INFOS[0],
                     "genome_info": GENOME_OBJ_INFOS[0]},
                    {"genome_ref": genome_refs[1],
                     "assembly_ref": assembly_refs[1],
                     "assembly_path": container_dir / ASSEMBLY_NAMES[1],
                     "assembly_info": ASSEMBLY_OBJ_INFOS[1],
                     "genome_info": GENOME_OBJ_INFOS[1]}]
    }
    gfu.genbanks_to_genomes.return_value = genbanks_to_genomes_results

    with patch.object(workspace_uploader, '_JOB_DIR_IN_CONTAINER', new=container_dir):
        uploaded_count = workspace_uploader._upload_objects_in_parallel(
            ws=ws,
            upload_env_key="CI",
            workspace_id=72231,
            load_id="214",
            ws_coll_src_dir=collection_source_dir,
            wait_to_upload_objs=wait_to_upload_genomes,
            batch_size=2,
            source_data_dir=sourcedata_dir,
            asu_client=Mock(),
            gfu_client=gfu,
            job_data_dir=container_dir,
        )

    assert uploaded_count == 2

    # assert that no interactions occurred with ws
    ws.get_object_info3.assert_not_called()

    # assert that asu was called correctly
    gfu.genbanks_to_genomes.assert_called_once_with(
        {
            "workspace_id": 72231,
            "inputs": [
                {
                    "file": {"path": f"{container_dir}/GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz"},
                    "genome_name": "GCF_000979855.1_gtlEnvA5udCFS_genomic.gbff.gz",
                    "metadata": {"load_id": "214"},
                },
                {
                    "file": {"path": f"{container_dir}/GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz"},
                    "genome_name": "GCF_000979175.1_gtlEnvA5udCFS_genomic.gbff.gz",
                    "metadata": {"load_id": "214"},
                }
            ]
        }
    )

    assembly_upa_dirs = [obj_info_to_upa(info, underscore_sep=True) for info in ASSEMBLY_OBJ_INFOS]

    # check softlink for post_process
    assert (os.readlink(os.path.join(collection_source_dir, assembly_upa_dirs[0]))
            == os.path.join(sourcedata_dir, assembly_upa_dirs[0]))

    assert (os.readlink(os.path.join(collection_source_dir, assembly_upa_dirs[1]))
            == os.path.join(sourcedata_dir, assembly_upa_dirs[1]))

    # check hardlink for post_process
    assert os.path.samefile(
        genome_collection_source_dirs[0] / ASSEMBLY_NAMES[0],
        os.path.join(sourcedata_dir, assembly_upa_dirs[0], f"{assembly_upa_dirs[0]}.fna.gz")
    )

    assert os.path.samefile(
        genome_collection_source_dirs[1] / ASSEMBLY_NAMES[1],
        os.path.join(sourcedata_dir, assembly_upa_dirs[1], f"{assembly_upa_dirs[1]}.fna.gz")
    )

    # check metadata file
    metadata_file = sourcedata_dir / assembly_upa_dirs[0] / f"{assembly_upa_dirs[0]}.meta"
    with open(metadata_file, 'r') as file:
        data = json.load(file)

    expected_metadata = {
        'upa': assembly_refs[0],
        'name': ASSEMBLY_NAMES[0],
        'timestamp': ASSEMBLY_OBJ_INFOS[0][3],
        'type': ASSEMBLY_OBJ_INFOS[0][2],
        'genome_upa': genome_refs[0],
        'assembly_object_info': ASSEMBLY_OBJ_INFOS[0],
        'genome_object_info': GENOME_OBJ_INFOS[0]
    }
    assert data == expected_metadata

    metadata_file = sourcedata_dir / assembly_upa_dirs[1] / f"{assembly_upa_dirs[1]}.meta"
    with open(metadata_file, 'r') as file:
        data = json.load(file)

    expected_metadata = {
        'upa': assembly_refs[1],
        'name': ASSEMBLY_NAMES[1],
        'timestamp': ASSEMBLY_OBJ_INFOS[1][3],
        'type': ASSEMBLY_OBJ_INFOS[1][2],
        'genome_upa': genome_refs[1],
        'assembly_object_info': ASSEMBLY_OBJ_INFOS[1],
        'genome_object_info': GENOME_OBJ_INFOS[1]
    }
    assert data == expected_metadata


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


def test_query_workspace_with_load_id_mass_genome(setup_and_teardown):
    # Test case 1: Valid scenario - test with genome objects
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

