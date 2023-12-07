import os
import time
import uuid
from multiprocessing import Pool, Queue
from typing import Callable, Tuple, Union

import docker

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.SampleServiceClient import SampleService
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_helper
from src.loaders.common.loader_common_names import CALLBACK_IMAGE_NAME


class Conf:
    """
    Configuration class for the workspace downloader and workspace uploader scripts.
    """

    def __init__(
        self,
        job_dir: str,
        output_dir: str,
        kb_base_url: str = "https://ci.kbase.us/services/",
        token_filepath: str | None = None,
        au_service_ver: str = "release",
        workers: int = 5,
        max_task: int = 20,
        worker_function: Callable | None = None,
        retrieve_sample: bool = False,
        ignore_no_sample_error: bool = False,
        workspace_downloader: bool = True,
    ) -> None:
        """
        Initialize the configuration class.

        Args:
            job_dir (str): The directory for SDK jobs per user.
            output_dir (str): The directory for a specific workspace id under sourcedata/ws.
            kb_base_url (str): The base url of the KBase services.
            token_filepath (str): The file path that stores a KBase token appropriate for the KBase environment.
                                If not supplied, the token must be provided in the environment variable KB_AUTH_TOKEN.
            au_service_ver (str): The service verison of AssemblyUtilClient
                                ('dev', 'beta', 'release', or a git commit).
            workers (int): The number of workers to use for multiprocessing.
            max_task (int): The maxmium subtasks for the callback server.
            worker_function (Callable): The function that will be called by the workers.
            retrieve_sample (bool): Whether to retrieve sample for each genome object.
            ignore_no_sample_error (bool): Whether to ignore the error when no sample data is found.
            workspace_downloader (bool): Whether to be used for the workspace downloader script.
        """
        ipv4 = loader_helper.get_ip()
        port = loader_helper.find_free_port()

        # common instance variables
        # the url of the workspace to contact
        self.ws_url = os.path.join(kb_base_url, "ws")

        # a KBase token appropriate for the KBase environment
        self.token = loader_helper.get_token(token_filepath)

        # setup and run callback server container
        self._start_callback_server(
            docker.from_env(),
            uuid.uuid4().hex,
            job_dir,
            kb_base_url,
            self.token,
            port,
            max_task,
            ipv4,
        )

        # the url of the callback service to contact
        self.callback_url = "http://" + ipv4 + ":" + str(port)
        print("callback_url:", self.callback_url)

        # the directory for SDK jobs per user
        self.job_data_dir = loader_helper.make_job_data_dir(job_dir)

        # unique to downloader
        if workspace_downloader:
            if worker_function is None:
                raise ValueError(
                    "worker_function cannot be None for the workspace downloader script"
                )

            # queue for the workspace downloader tasks
            self.input_queue = Queue()

            # the directory for a specific workspace id under sourcedata/ws
            self.output_dir = output_dir

            # whether to retrieve sample for each genome object
            self.retrieve_sample = retrieve_sample

            # whether to ignore the error when no sample data is found
            self.ignore_no_sample_error = ignore_no_sample_error

            # Workspace client
            self.ws = Workspace(self.ws_url, token=self.token)

            # AssemblyUtil client
            self.asu = AssemblyUtil(
                self.callback_url, service_ver=au_service_ver, token=self.token
            )

            # SampleService client
            sample_url = os.path.join(kb_base_url, "sampleservice")
            self.ss = SampleService(sample_url, token=self.token)

            # a pool of worker processes.
            self.pools = Pool(workers, worker_function, [self])

    def _setup_callback_server_envs(
        self,
        job_dir: str,
        kb_base_url: str,
        token: str,
        port: int,
        max_task: int,
        ipv4: str,
    ) -> Tuple[dict[str, Union[int, str]], dict[str, dict[str, str]]]:
        """
        Setup the environment variables and volumes for the callback server.

        Args:
            job_dir (str): The directory for SDK jobs per user.
            kb_base_url (str): The base url of the KBase services.
            token (str): The KBase token. Also used as a catalog admin token if valid.
            port (int): The port number for the callback server.
            max_task (int): The maxmium subtasks for the callback server.
            ipv4: (str): The ipv4 address for the callback server.

        Returns:
            tuple: A tuple of the environment variables and volumes for the callback server.
        """
        # initiate env and vol
        env = {}
        vol = {}

        # used by the callback server
        env["KB_AUTH_TOKEN"] = token
        env["KB_ADMIN_AUTH_TOKEN"] = token  # pass in catalog admin token to get catalog params
        env["KB_BASE_URL"] = kb_base_url
        env["JOB_DIR"] = job_dir
        env["CALLBACK_PORT"] = port
        env["JR_MAX_TASKS"] = max_task
        env["CALLBACK_IP"] = ipv4  # specify an ipv4 address for the callback server
                                   # otherwise, the callback container will use the an ipv6 address

        # setup volumes required for docker container
        docker_host = os.environ["DOCKER_HOST"]
        if docker_host.startswith("unix:"):
            docker_host = docker_host[5:]

        vol[job_dir] = {"bind": job_dir, "mode": "rw"}
        vol[docker_host] = {"bind": "/run/docker.sock", "mode": "rw"}

        return env, vol

    def _start_callback_server(
        self,
        client: docker.client,
        container_name: str,
        job_dir: str,
        kb_base_url: str,
        token: str,
        port: int,
        max_task: int,
        ipv4: str,
    ) -> None:
        """
        Start the callback server.

        Args:
            client (docker.client): The docker client.
            container_name (str): The name of the container.
            job_dir (str): The directory for SDK jobs per user.
            kb_base_url (str): The base url of the KBase services.
            token (str): The KBase token.
            max_task (int): The maxmium subtasks for the callback server.
            port (int): The port number for the callback server.
            ipv4: (str): The ipv4 address for the callback server.
        """
        env, vol = self._setup_callback_server_envs(
            job_dir, kb_base_url, token, port, max_task, ipv4
        )
        self.container = client.containers.run(
            name=container_name,
            image=CALLBACK_IMAGE_NAME,
            detach=True,
            network_mode="host",
            environment=env,
            volumes=vol,
        )
        time.sleep(2)

    def _get_container_logs(self) -> None:
        """
        Get logs from the callback server container.
        """
        logs = self.container.logs()
        if logs:
            print("\n****** Logs from the Callback Server ******\n")
            logs = logs.decode("utf-8")
            for line in logs.split("\n"):
                print(line)

    def stop_callback_server(self) -> None:
        """
        Stop the callback server.
        """
        self._get_container_logs()
        self.container.stop()
        self.container.remove()