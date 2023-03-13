import datetime
import fcntl
import os
import pathlib
import shutil
from enum import Enum

import jsonlines
import pandas as pd

import src.loaders.jobs.taskfarmer.taskfarmer_common as tf_common
from src.loaders.common import loader_common_names

# Produced once all tasks are completed. https://docs.nersc.gov/jobs/workflow/taskfarmer/#output
NERSC_SLURM_DONE_FILE = 'done.tasks.txt.tfin'
REQUIRED_TASK_INFO_KEYS = ['kbase_collection', 'load_ver', 'tool', 'job_id', 'job_submit_time', 'source_data_dir']


class JobStatus(Enum):
    """
    Job status.
    """
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class TFTaskManager:
    """
    Task manager for TaskFarmer jobs.

    The task manager is responsible for managing the tasks of TaskFarmer jobs.
    """

    def _get_task_info_file(self):
        """
        Get the task info file.

        The task info file is located under the TASKFARMER_JOB_DIR directory.
        The file name is retrieved from TASK_INFO_FILE.
        """
        task_info_file = os.path.join(self.root_dir, tf_common.TASKFARMER_JOB_DIR, tf_common.TASK_INFO_FILE)

        pathlib.Path(task_info_file).touch(exist_ok=True)

        return task_info_file

    def _retrieve_all_tasks(self):
        """
        Retrieve the task information from the task info file matching the kbase_collection, load_ver, and tool.

        Sort the tasks by job_submit_time in descending order.
        """
        if not self._task_exists:
            return None

        task_info_file = self._get_task_info_file()
        df = pd.read_json(task_info_file, lines=True)

        tasks_df = df[(df["kbase_collection"] == self.kbase_collection) &
                      (df["load_ver"] == self.load_ver) &
                      (df["tool"] == self.tool)]

        tasks_df.sort_values(by="job_submit_time", ascending=False, inplace=True)

        return tasks_df

    def _create_job_dir(self, destroy_old_job_dir=False):
        """
        Create the job directory. If destroy_old_job_dir is True, recreate the job directory.
        """

        if os.path.exists(self.job_dir) and destroy_old_job_dir:
            print(f'removing job dir {self.job_dir}')
            shutil.rmtree(self.job_dir, ignore_errors=True)

        os.makedirs(self.job_dir, exist_ok=True)

        return self.job_dir

    def _get_job_status_from_nersc(self, job_id):
        """
        Get the job status from NERSC using squeue command.

        A 'done.tasks.txt.tfin' file is created in the job directory when the job is finished successfully.

        When job is queued, squeue command will return code 0 and out put will be like (with no job info just the header):
        JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)


        When job is running, squeue command will return code 0 and out put will be like:
        JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
        5987779 regular_m submit_t      tgu  R       1:20      2 nid[005689-005690]

        When job is finished/failed:
            * job status is cached for a period of time (about 10 mins). squeue command will return code 0 and
            out put will be like below (with no job info just the header):
            JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)

            * after the period of time, squeue command will return error code 1 and error message:
            slurm_load_jobs error: Invalid job id specified

        Matrix of job status:
        Done file  Exit Code   Job listed    Status
        Y          Any         Any           COMPLETED
        N          0           N             PENDING or FAILED (if failed, exit code will flip to -1 within ~10m)
        N          0           Y             RUNNING
        N          -1          N/A           FAILED

        NOTE: If job fails and squeue command hasn't flipped the exit code to -1, this function will return 'PENDING'
        as the job status. After about 10 mins, the exit code will be flipped to -1 and this function will return
        'FAILED' status as the job status.
        """

        # Once all tasks are completed, NERSC generates this file.
        # (ref: https://docs.nersc.gov/jobs/workflow/taskfarmer/#output)
        done_file = os.path.join(self.job_dir, NERSC_SLURM_DONE_FILE)
        if os.path.isfile(done_file):
            return JobStatus.COMPLETED

        # check job status using squeue command
        std_out_file, std_err_file, exit_code = tf_common.run_nersc_command(
            ['squeue', '-j', str(job_id)], self.job_dir, log_file_prefix='squeue', check_return_code=False)

        if exit_code != 0:
            # job finished without creating the done file indicating the job is failed
            return JobStatus.FAILED

        with open(std_out_file, "r") as std_out, open(std_err_file, "r") as std_err:
            squeue_out = std_out.read().strip()
            if str(job_id) in squeue_out:
                return JobStatus.RUNNING
            else:
                return JobStatus.PENDING

    def _append_task_info(self, task_info):
        """
        Append the task information to the task info file
        """

        task_info.update({'kbase_collection': self.kbase_collection, 'load_ver': self.load_ver, 'tool': self.tool})

        if not all(key in task_info for key in REQUIRED_TASK_INFO_KEYS):
            raise ValueError(f"task_info must contain all keys: {REQUIRED_TASK_INFO_KEYS}")

        # Append the new record to the file
        task_info_file = self._get_task_info_file()
        with jsonlines.open(task_info_file, mode='a') as writer:
            # lock the file to prevent multiple processes from writing to the same file
            fcntl.flock(writer, fcntl.LOCK_EX)
            writer.write(task_info)
            fcntl.flock(writer, fcntl.LOCK_UN)

    def __init__(self, kbase_collection, load_ver, tool, root_dir=loader_common_names.ROOT_DIR,
                 destroy_old_job_dir=False):
        """
        Initialize the task manager.

        :param kbase_collection: KBase collection identifier name (e.g. GTDB).
        :param load_ver: collection load version (e.g. r207.kbase.1).
        :param tool: tool name (e.g. gtdb_tk, checkm2, etc.)
        :param root_dir: root directory of the collection project.
                         Default is the ROOT_DIR defined in src/loaders/common/loader_common_names.py
        :param destroy_old_job_dir: if True, remove contents of the old job directory.
        """

        self.kbase_collection = kbase_collection
        self.load_ver = load_ver
        self.tool = tool
        self.root_dir = root_dir

        # job directory is named as <kbase_collection>_<load_ver>_<tool>
        self.job_dir = os.path.join(self.root_dir, tf_common.TASKFARMER_JOB_DIR,
                                    f'{self.kbase_collection}_{self.load_ver}_{self.tool}')
        self._task_exists = os.path.isdir(self.job_dir) and os.listdir(self.job_dir)
        self._create_job_dir(destroy_old_job_dir=destroy_old_job_dir)
        self._tasks_df = self._retrieve_all_tasks()

    def get_latest_task(self):
        """
        Get the latest task information

        Tasks are sorted by job_submit_time in descending order. The latest task is the first row.
        """

        return self._tasks_df.iloc[0].to_dict() if self._task_exists else {}

    def retrieve_latest_task_status(self):
        """
        Retrieve the latest task status.

        """
        if not self._task_exists:
            raise ValueError(f"Task does not exist for {self.kbase_collection}, {self.load_ver}, {self.tool}")

        latest_task = self.get_latest_task()
        job_status = self._get_job_status_from_nersc(latest_task["job_id"])

        return job_status

    def submit_job(self, source_data_dir):
        """
        Submit the job to slurm
        """
        os.chdir(self.job_dir)

        # If the .tfin files exist (these files are created by the previous run),
        # delete them before starting a new run to avoid any issues caused by the previous run's files.
        # TODO: investigate whether it is necessary to delete the .tfin files if a job fails due to walltime.
        for filename in os.listdir(self.job_dir):
            if filename.endswith(".tfin"):
                os.remove(os.path.join(self.job_dir, filename))

        current_datetime = datetime.datetime.now()
        std_out_file, std_err_file, exit_code = tf_common.run_nersc_command(
            ['sbatch', os.path.join(self.job_dir, 'submit_taskfarmer.sl')],
            self.job_dir, log_file_prefix='sbatch_submit')
        with open(std_out_file, "r") as f:
            sbatch_out = f.read().strip()
            job_id = sbatch_out.split(' ')[-1]
            print(f'Job submitted to slurm.\n{sbatch_out}')

        task_info = {'job_id': job_id, 'job_submit_time': current_datetime.strftime("%Y-%m-%d %H:%M:%S"),
                     'source_data_dir': source_data_dir}
        self._append_task_info(task_info)

        self._task_exists = True
        self._tasks_df = self._retrieve_all_tasks()

        return job_id
