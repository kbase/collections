import os
import shutil
from enum import Enum

import pandas as pd

import src.loaders.jobs.taskfarmer.taskfarmer_common as tf_common

NERSC_SLURM_DONE_FILE = 'done.tasks.txt.tfin'


class JobStatus(Enum):
    """
    Job status.
    """
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class TFTaskManager:

    def __init__(self, kbase_collection, load_ver, tool, root_dir):
        self.kbase_collection = kbase_collection
        self.load_ver = load_ver
        self.tool = tool
        self.root_dir = root_dir

        self.job_dir = self.get_job_dir()
        self.task_exists = os.path.isdir(self.job_dir)
        self.tasks_df = self.retrieve_task_info()

    def get_job_dir(self):
        """
        Get the job directory for the kbase_collection, load_ver, and tool.

        The job directory is named as <kbase_collection>_<load_ver>_<tool>.
        """
        job_dir = os.path.join(self.root_dir, tf_common.TASKFARMER_JOB_DIR,
                               f'{self.kbase_collection}_{self.load_ver}_{self.tool}')

        return job_dir

    def recreate_job_dir(self):
        """
        Recreate the job directory.
        """

        print(f'recreating job dir {self.job_dir}')
        if os.path.exists(self.job_dir):
            shutil.rmtree(self.job_dir)
        os.makedirs(self.job_dir, exist_ok=True)

        return self.job_dir

    def create_job_dir(self):
        """
        Create the job directory.
        """
        os.makedirs(self.job_dir, exist_ok=True)

        return self.job_dir

    def get_task_info_file(self):
        """
        Get the task info file.

        The task info file is located under the TASKFARMER_JOB_DIR directory.
        The file name is retrieved from TASK_INFO_FILE.
        """
        task_info_file = os.path.join(self.root_dir, tf_common.TASKFARMER_JOB_DIR, tf_common.TASK_INFO_FILE)

        if not os.path.exists(task_info_file):
            with open(task_info_file, 'w') as f:
                pass  # creates an empty file

        return task_info_file

    def retrieve_task_info(self):
        """
        Retrieve the task information from the task info file matching the kbase_collection, load_ver, and tool.

        Sort the tasks by job_start_time in descending order.
        """
        if not self.task_exists:
            return None

        task_info_file = self.get_task_info_file()
        df = pd.read_json(task_info_file, lines=True)

        tasks_df = df[(df["kbase_collection"] == self.kbase_collection) &
                      (df["load_ver"] == self.load_ver) &
                      (df["tool"] == self.tool)]

        tasks_df.sort_values(by="job_start_time", ascending=False, inplace=True)

        return tasks_df

    def get_latest_task(self):
        """
        Get the latest task information

        Tasks are sorted by job_start_time in descending order. The latest task is the first row.
        """
        if not self.task_exists:
            return {}

        return self.tasks_df.iloc[0].to_dict()

    def get_job_status_fm_nersc(self, job_id):
        """
        Get the job status from NERSC using squeue command.

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
        """

        std_out_file, std_err_file, exit_code = tf_common.run_nersc_command(['squeue', '-j', str(job_id)],
                                                                            self.job_dir, log_file_prefix='squeue',
                                                                            check_return_code=False)

        if exit_code != 0:
            # job (successfully) finished status is checked by checking the `done.tasks.txt.tfin` file in job directory
            return JobStatus.FAILED

        with open(std_out_file, "r") as std_out, open(std_err_file, "r") as std_err:
            squeue_out = std_out.read().strip()
            if str(job_id) in squeue_out:
                return JobStatus.RUNNING
            else:
                return JobStatus.PENDING

    def retrieve_latest_task_status(self):
        """
        Retrieve the latest task status.

        """
        if not self.task_exists:
            raise ValueError(f"Task does not exist for {self.kbase_collection}, {self.load_ver}, {self.tool}")

        latest_task = self.get_latest_task()

        done_file = os.path.join(self.job_dir, NERSC_SLURM_DONE_FILE)
        if os.path.isfile(done_file):
            job_status = JobStatus.COMPLETED
        else:
            job_id = latest_task["job_id"]
            job_status = self.get_job_status_fm_nersc(job_id)

        return job_status
