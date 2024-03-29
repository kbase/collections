import datetime
import fcntl
import json
import os
import pathlib
import time
from enum import Enum

import pandas as pd
from file_read_backwards import FileReadBackwards

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

        If the file does not exist, return an empty list.
        """

        if not os.path.exists(file_path):
            return []

        with FileReadBackwards(file_path, encoding="utf-8") as file:

            last_lines = []
            for line in file:
                last_lines.append(line.rstrip())
                if len(last_lines) == n:
                    break

        return last_lines

    def _get_task_info_file(self):
        """
        Get the task info file.

        The task info file is located under the TASKFARMER_JOB_DIR directory.
        The file name is retrieved from TASK_INFO_FILE.
        """

        # create the taskfarmer job dir if it does not exist
        task_info_dir = os.path.join(self.root_dir, TASKFARMER_JOB_DIR)
        os.makedirs(task_info_dir, exist_ok=True)

        task_info_file = os.path.join(task_info_dir, TASK_INFO_FILE)

        pathlib.Path(task_info_file).touch(exist_ok=True)

        return task_info_file

    def _retrieve_all_tasks(self):
        """
        Retrieve the task information from the task info file matching the kbase_collection, load_ver, env, and tool.

        Sort the tasks by job_submit_time in descending order. Return the tasks as a pandas DataFrame. If no tasks are
        found, return an empty DataFrame.
        """
        task_info_file = self._get_task_info_file()
        # ensure to use str type for all columns in case of numeric values, e.g. load_ver = 2023.01
        df = pd.read_json(task_info_file, lines=True, dtype=str)

        if not df.empty:
            df = df[(df["kbase_collection"] == self.kbase_collection) &
                    (df["load_ver"] == self.load_ver) &
                    (df["env"] == self.env) &
                    (df["tool"] == self.tool)]

            df.sort_values(by="job_submit_time", ascending=False, inplace=True)

        return df

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

        When job is timed out, time out message will be written to the job slurm log file (last line).
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
            # job finished without creating the done file indicating the job is failed (cancelled) or timed out
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

        task_info.update({'kbase_collection': self.kbase_collection,
                          'load_ver': self.load_ver,
                          'env': self.env,
                          'tool': self.tool,
                          'source_data_dir': self.source_data_dir})

        # Append the new record to the file
        task_info_file = self._get_task_info_file()
        with open(task_info_file, 'a') as writer:
            fcntl.flock(writer.fileno(), fcntl.LOCK_EX)
            json.dump(task_info, writer)
            writer.write('\n')
            fcntl.flock(writer.fileno(), fcntl.LOCK_UN)

    def _cancel_job(self, job_id):
        """
        Cancel a job on NERSC.
        """

        job_status, retry = self._get_job_status_from_nersc(job_id), 0

        if job_status not in [JobStatus.RUNNING, JobStatus.PENDING]:
            return True

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
            print(f"Job {job_id} is canceled.")

    def _get_latest_task(self):
        """
        Get the latest task information from the task info file. Also retrieve the job status from NERSC.

        Tasks are sorted by job_submit_time in descending order. The latest task is the first row.
        """

        tasks_df = self._retrieve_all_tasks()
        latest_task = tasks_df.iloc[0].to_dict() if not tasks_df.empty else {}

        if latest_task:
            job_status = self._get_job_status_from_nersc(latest_task["job_id"])
            latest_task['job_status'] = job_status

        return latest_task

    def _check_preconditions(self, restart_on_demand):
        """
        Check conditions from previous runs of the same tool and load version

        :param restart_on_demand: if True, killed any running/pending jobs and restart running the tool again.
        """

        latest_task = self._get_latest_task()

        if not latest_task:
            return True

        latest_task_status, job_id = latest_task['job_status'], latest_task['job_id']

        if restart_on_demand:
            # cancel the previous job if it is running or pending
            if latest_task_status in [JobStatus.RUNNING, JobStatus.PENDING]:
                self._cancel_job(job_id)

            # make sure all .tfin files from previous run are removed to avoid any issues caused by them
            for filename in os.listdir(self.job_dir):
                if filename.endswith(".tfin"):
                    os.remove(os.path.join(self.job_dir, filename))

            return True

        if latest_task['source_data_dir'] != self.source_data_dir:
            raise PreconditionError(
                f'There is a previous run of the same tool and load version with a different source data directory.')

        if latest_task_status in [JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.TIMEOUT]:
            print(f'The tool and load version have been run before, '
                  f'and the most recent status is {str(latest_task_status)}.'
                  f'Resuming progress from the previous run.')
        elif latest_task_status in [JobStatus.COMPLETED, JobStatus.RUNNING, JobStatus.PENDING]:
            raise PreconditionError(
                f'There is a previous run of the same tool and load version that is {str(latest_task_status)}.')
        else:
            raise ValueError(f'Unexpected job status: {latest_task_status}')

    def __init__(self,
                 kbase_collection,
                 load_ver,
                 env,
                 tool,
                 source_data_dir,
                 restart_on_demand,
                 root_dir=loader_common_names.ROOT_DIR):
        """
        Initialize the task manager.

        :param kbase_collection: KBase collection identifier name (e.g. GTDB).
        :param load_ver: collection load version (e.g. r207.kbase.1).
        :param env: environment name (e.g. PROD, CI, etc.)
        :param tool: tool name (e.g. gtdb_tk, checkm2, etc.)
        :param source_data_dir: source data directory.
        :param restart_on_demand: if True, killed any running/pending jobs and restart running the tool again.
        :param root_dir: root directory of the collection project.
                         Default is the ROOT_DIR defined in src/loaders/common/loader_common_names.py

        """

        self.kbase_collection = kbase_collection
        self.load_ver = load_ver
        self.env = env
        self.tool = tool
        self.source_data_dir = source_data_dir
        self.root_dir = root_dir
        self.restart_on_demand = restart_on_demand

        # job directory is named as env/<kbase_collection>_<load_ver>_<tool>
        self.job_dir = os.path.join(
            self.root_dir, TASKFARMER_JOB_DIR, self.env, f'{self.kbase_collection}_{self.load_ver}_{self.tool}')

        self._check_preconditions(self.restart_on_demand)

    def submit_job(self):
        """
        Submit the job to slurm

        Follow the steps in https://docs.nersc.gov/jobs/workflow/taskfarmer/#taskfarmer and generate all the necessary
        files for the job submission (e.g. wrapper script, task list and batch script, etc.) before calling this
        function.
        """

        # check if all the required files exist
        required_files = [tf_common.WRAPPER_FILE, tf_common.TASK_FILE, tf_common.BATCH_SCRIPT]
        for filename in required_files:
            if not os.path.isfile(os.path.join(self.job_dir, filename)):
                raise ValueError(f"{filename} does not exist in {self.job_dir}")

        self._check_preconditions(self.restart_on_demand)

        current_datetime = datetime.datetime.now()
        std_out_file, std_err_file, exit_code = tf_common.run_nersc_command(
            ['sbatch', os.path.join(self.job_dir, tf_common.BATCH_SCRIPT)], self.job_dir,
            log_file_prefix='sbatch_submit')
        with open(std_out_file, "r") as f:
            sbatch_out = f.read().strip()
            job_id = sbatch_out.split(' ')[-1]
            print(f'Job submitted to slurm.\n{sbatch_out}')

        task_info = {'job_id': job_id, 'job_submit_time': current_datetime.strftime("%Y-%m-%d %H:%M:%S")}
        self._append_task_info(task_info)

        return job_id
