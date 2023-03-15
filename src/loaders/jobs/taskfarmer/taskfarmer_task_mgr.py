import datetime
import fcntl
import json
import os
import pathlib
import shutil
import time
from enum import Enum

import pandas as pd

import src.loaders.jobs.taskfarmer.taskfarmer_common as tf_common
from src.loaders.common import loader_common_names

# Produced once all tasks are completed. https://docs.nersc.gov/jobs/workflow/taskfarmer/#output
NERSC_SLURM_DONE_FILE = 'done.tasks.txt.tfin'
REQUIRED_TASK_INFO_KEYS = ['job_id', 'job_submit_time']

# directory containing the TaskFarmer job scripts under the root directory
TASKFARMER_JOB_DIR = 'task_farmer_jobs'
# file containing the information of each task
TASK_INFO_FILE = 'task_info.jsonl'

# specific string present in the Slurm log file which indicates that the job has timed out
TIMEOUT_STR = 'DUE TO TIME LIMIT'


class TaskError(Exception):
    pass


class PreconditionError(TaskError):
    pass


class JobStatus(Enum):
    """
    Job status.
    """
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED/CANCELLED'  # in many situations, we cannot identify whether a job has failed or cancelled
    CANCELLED = 'CANCELLED'
    TIMEOUT = 'TIMEOUT'

    def __str__(self):
        return self.value.lower()


class TFTaskManager:
    """
    Task manager for TaskFarmer jobs.

    The task manager is responsible for managing the tasks of TaskFarmer jobs.
    """

    @staticmethod
    def _read_last_n_line(file_path, n=5):
        """
        Read the last n lines from a file and return them as a list of strings.
        """

        if not os.path.exists(file_path):
            return []

        with open(file_path, 'r') as file:
            file.seek(0, os.SEEK_END)
            position = file.tell()

            last_lines = []
            while len(last_lines) < n and position >= 0:
                position -= 1
                file.seek(position, os.SEEK_SET)
                character = file.read(1)
                if character == '\n':
                    line = file.readline().strip()
                    last_lines.insert(0, line)
                elif position == 0:  # reached the beginning of the file
                    line = file.readline().strip()
                    last_lines.insert(0, line)

        return last_lines

    def _get_task_info_file(self):
        """
        Get the task info file.

        The task info file is located under the TASKFARMER_JOB_DIR directory.
        The file name is retrieved from TASK_INFO_FILE.
        """
        task_info_file = os.path.join(self.root_dir, TASKFARMER_JOB_DIR, TASK_INFO_FILE)

        pathlib.Path(task_info_file).touch(exist_ok=True)

        return task_info_file

    def _retrieve_all_tasks(self):
        """
        Retrieve the task information from the task info file matching the kbase_collection, load_ver, and tool.

        Sort the tasks by job_submit_time in descending order.
        """
        if not self._task_exists():
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

    def _check_time_out(self, job_id):
        """
        Check if the job has timed out
        """

        slurm_log = os.path.join(self.job_dir, f'slurm-{job_id}.out')
        slurm_last_lines = self._read_last_n_line(slurm_log)

        return TIMEOUT_STR in ''.join(slurm_last_lines)

    def _get_job_status_from_nersc(self, job_id):
        """
        Get the job status from NERSC using squeue command.

        A 'done.tasks.txt.tfin' file is created in the job directory when the job is finished successfully.

        When job is timed out, time out message will be written to the job slurm log file.
        time out message looks like:
        slurmstepd: error: *** STEP 6075199.0 ON nid004241 CANCELLED AT 2023-03-15T16:33:05 DUE TO TIME LIMIT ***

        When job is queued, squeue command will return code 0 and out put will be like:
        JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
        6049897 regular_m submit_t      tgu PD       0:00      2 (Priority)

        When job is running, squeue command will return code 0 and out put will be like:
        JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
        5987779 regular_m submit_t      tgu  R       1:20      2 nid[005689-005690]

        When job is finished/failed:
            * job is cached for a period of time (about 10 mins). squeue command will return code 0 and
            out put will be like below (with no job info just the header):
            JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)

            * after the period of time, squeue command will return error code 1 and error message:
            slurm_load_jobs error: Invalid job id specified

        When job is cancelled:
            * during cancellation, squeue command will return code 0 and out put will be like:
            JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
            6050963 regular_m submit_t      tgu CG       1:17      2 nid[004477-004478]
            * after cancellation, job is cached for a period of time (about 10 mins).
            squeue command will return code 0 and out put will be like below (with no job info just the header):
            JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
            * after the period of time, squeue command will return error code -1 and error message:
            slurm_load_jobs error: Invalid job id specified

        Matrix of job status:
        Done file  Time out string Exit Code   Job listed    Job Status Code  Returned Status
        Y           N              Any         N/A           N/A              COMPLETED
        N           N              0           Y             PD               PENDING
        N           N              0           Y             R                RUNNING
        N           N              -1          N/A           N/A              FAILED/CANCELLED (If a job is cancelled, it will be treated as failed.)
        N           N              0           N             N/A              FAILED/CANCELLED (If a job is cancelled, it will be treated as failed.)
        N           N              0           Y             CG               CANCELLED
        N           Y              Any         N/A           N/A              TIMEOUT
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
            return JobStatus.TIMEOUT if self._check_time_out(job_id) else JobStatus.FAILED

        with open(std_out_file, "r") as std_out, open(std_err_file, "r") as std_err:
            squeue_out = std_out.read().strip()
            if str(job_id) in squeue_out:
                job_str = squeue_out.splitlines()[1]
                parsed_status = job_str.split()[4]
                if parsed_status == 'PD':
                    return JobStatus.PENDING
                elif parsed_status == 'R':
                    return JobStatus.RUNNING
                elif parsed_status == 'CG':
                    return JobStatus.CANCELLED
                else:
                    raise ValueError(f"Unrecognized job status: {squeue_out}")
            else:
                return JobStatus.TIMEOUT if self._check_time_out(job_id) else JobStatus.FAILED

    def _append_task_info(self, task_info):
        """
        Append the task information to the task info file
        """

        if not all(key in task_info for key in REQUIRED_TASK_INFO_KEYS):
            raise ValueError(f"task_info must contain all keys: {REQUIRED_TASK_INFO_KEYS}")

        task_info.update({'kbase_collection': self.kbase_collection, 'load_ver': self.load_ver, 'tool': self.tool,
                          'source_data_dir': self.source_data_dir})

        # Append the new record to the file
        task_info_file = self._get_task_info_file()
        with open(task_info_file, 'a') as writer:
            fcntl.flock(writer.fileno(), fcntl.LOCK_EX)
            json.dump(task_info, writer)
            writer.write('\n')
            fcntl.flock(writer.fileno(), fcntl.LOCK_UN)

    def _task_exists(self):
        """
        Check if the task for kbase_collection, load_ver, tool has been submitted.
        """

        if self.force_run:
            # The job directory is recreated as part of the initialization process.
            job_dir_exists = True
        else:
            # check if the job directory exists and is not empty
            job_dir_exists = os.path.isdir(self.job_dir) and os.listdir(self.job_dir)

        # check task exists in the task info file
        task_info_file = self._get_task_info_file()
        df = pd.read_json(task_info_file, lines=True)

        if df.empty:
            return False

        tasks_df = df[(df["kbase_collection"] == self.kbase_collection) &
                      (df["load_ver"] == self.load_ver) &
                      (df["tool"] == self.tool)]

        return job_dir_exists and not tasks_df.empty

    def _cancel_job(self, job_id):
        """
        Cancel a job on NERSC.
        """

        job_status, retry = self._get_job_status_from_nersc(job_id), 0

        while job_status in [JobStatus.RUNNING, JobStatus.PENDING] and retry < 3:
            retry += 1
            print(f"Canceling job {job_id} (try #{retry})...")
            tf_common.run_nersc_command(
                ['scancel', str(job_id)], self.job_dir, log_file_prefix='scancel', check_return_code=True)
            time.sleep(2)
            # Get the job status to confirm the job is canceled
            job_status = self._get_job_status_from_nersc(job_id)

        if job_status in [JobStatus.RUNNING, JobStatus.PENDING]:
            raise ValueError(f"Failed to cancel job {job_id}.")
        else:
            print(f"Job {job_id} is cancelled.")

    def _get_latest_task(self):
        """
        Get the latest task information

        Tasks are sorted by job_submit_time in descending order. The latest task is the first row.
        """
        return self._tasks_df.iloc[0].to_dict() if self._task_exists() else {}

    def _retrieve_latest_task_status(self):
        """
        Retrieve the latest task status.
        """
        if not self._task_exists():
            raise ValueError(f"Task does not exist for {self.kbase_collection}, {self.load_ver}, {self.tool}")

        latest_task = self._get_latest_task()
        job_id = latest_task["job_id"]
        job_status = self._get_job_status_from_nersc(job_id)

        return job_status, job_id

    def _check_preconditions(self):
        """
        Check conditions from previous runs of the same tool and load version
        """

        if not self._task_exists():
            return True

        latest_task = self._get_latest_task()
        latest_task_status, job_id = self._retrieve_latest_task_status()

        if self.force_run:
            # cancel the previous job if it is running or pending
            if latest_task_status in [JobStatus.RUNNING, JobStatus.PENDING]:
                self._cancel_job(job_id)
            return True

        if latest_task['source_data_dir'] != self.source_data_dir:
            raise PreconditionError(
                f'There is a previous run of the same tool and load version with a different source data directory.')

        if latest_task_status in [JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMEOUT]:
            print(f'The tool and load version have been run before, '
                  f'and the most recent status is {str(latest_task_status)}.\n'
                  f'Resuming progress from the previous run.')
        elif latest_task_status in [JobStatus.COMPLETED, JobStatus.RUNNING, JobStatus.PENDING]:
            raise PreconditionError(
                f'There is a previous run of the same tool and load version that is {str(latest_task_status)}.')
        else:
            raise ValueError(f'Unexpected job status: {latest_task_status}')

    def __init__(self, kbase_collection, load_ver, tool, source_data_dir, root_dir=loader_common_names.ROOT_DIR,
                 force_run=False):
        """
        Initialize the task manager.

        :param kbase_collection: KBase collection identifier name (e.g. GTDB).
        :param load_ver: collection load version (e.g. r207.kbase.1).
        :param tool: tool name (e.g. gtdb_tk, checkm2, etc.)
        :param source_data_dir: source data directory.
        :param root_dir: root directory of the collection project.
                         Default is the ROOT_DIR defined in src/loaders/common/loader_common_names.py
        :param force_run: if True, remove contents of the old job directory and run the job.
        """

        self.kbase_collection = kbase_collection
        self.load_ver = load_ver
        self.tool = tool
        self.source_data_dir = source_data_dir
        self.root_dir = root_dir
        self.force_run = force_run

        # job directory is named as <kbase_collection>_<load_ver>_<tool>
        self.job_dir = os.path.join(self.root_dir, TASKFARMER_JOB_DIR,
                                    f'{self.kbase_collection}_{self.load_ver}_{self.tool}')

        self._create_job_dir(destroy_old_job_dir=self.force_run)
        self._tasks_df = self._retrieve_all_tasks()

    def submit_job(self):
        """
        Submit the job to slurm

        Follow the steps in https://docs.nersc.gov/jobs/workflow/taskfarmer/#taskfarmer and generate all the necessary
        files for the job submission (e.g. wrapper script, task list and batch script, etc.) before calling this function.
        """

        # check if all the required files exist
        required_files = [tf_common.WRAPPER_FILE, tf_common.TASK_FILE, tf_common.BATCH_SCRIPT]
        for filename in required_files:
            if not os.path.isfile(os.path.join(self.job_dir, filename)):
                raise ValueError(f"{filename} does not exist in {self.job_dir}")

        self._check_preconditions()

        os.chdir(self.job_dir)

        current_datetime = datetime.datetime.now()
        std_out_file, std_err_file, exit_code = tf_common.run_nersc_command(
            ['sbatch', os.path.join(self.job_dir, tf_common.BATCH_SCRIPT)],
            self.job_dir, log_file_prefix='sbatch_submit')
        with open(std_out_file, "r") as f:
            sbatch_out = f.read().strip()
            job_id = sbatch_out.split(' ')[-1]
            print(f'Job submitted to slurm.\n{sbatch_out}')

        task_info = {'job_id': job_id, 'job_submit_time': current_datetime.strftime("%Y-%m-%d %H:%M:%S")}
        self._append_task_info(task_info)

        self._tasks_df = self._retrieve_all_tasks()

        return job_id
