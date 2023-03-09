import datetime
import os
import subprocess

# directory containing the TaskFarmer job scripts under the root directory
TASKFARMER_JOB_DIR = 'task_farmer_jobs'
# file containing the information of each task
TASK_INFO_FILE = 'task_info.jsonl'


def run_nersc_command(command, job_dir, log_file_prefix='', check_return_code=True):
    """
    Run a command on NERSC.

    The command is run in a subprocess. The standard output and standard error are written to files.
    The files are named as stdout_<log_file_prefix>_<timestamp> and stderr_<log_file_prefix>_<timestamp>.
    """

    log_dir = os.path.join(job_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    std_out_file = os.path.join(log_dir,
                                f'stdout_{log_file_prefix}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}')
    std_err_file = os.path.join(log_dir,
                                f'stderr_{log_file_prefix}_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}')

    with open(std_out_file, "w") as std_out, open(std_err_file, "w") as std_err:
        p = subprocess.Popen(command, stdout=std_out, stderr=std_err, text=True)

    exit_code = p.wait()

    if check_return_code and exit_code != 0:
        with open(std_out_file, "r") as std_out, open(std_err_file, "r") as std_err:
            raise ValueError(f"Error running command '{command}'.\n"
                             f"Standard output: {std_out.read()}\n"
                             f"Standard error: {std_err.read()}")

    return std_out_file, std_err_file, exit_code
