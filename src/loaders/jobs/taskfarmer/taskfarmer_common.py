import datetime
import os
import subprocess

# The wrapper file that specifies the executable and necessary options for the task
# https://docs.nersc.gov/jobs/workflow/taskfarmer/#step-1-write-a-wrapper-wrappersh
WRAPPER_FILE = 'shifter_wrapper.sh'

# The task file that lists all tasks to be run
# https://docs.nersc.gov/jobs/workflow/taskfarmer/#step-2-create-a-task-list-taskstxt
TASK_FILE = 'tasks.txt'

# The name of the script that submits the job to NERSC
# https://docs.nersc.gov/jobs/workflow/taskfarmer/#step-3-create-a-batch-script-submit_taskfarmersl
BATCH_SCRIPT = 'submit_taskfarmer.sl'


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
