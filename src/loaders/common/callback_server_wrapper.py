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

    Instance variables:

    token - a KBase token appropriate for the KBase environment
    callback_url - the url of the callback service to contact
    job_data_dir - the directory for SDK jobs per user
    input_queue - queue for the workspace downloader tasks
    output_dir - the directory for a specific workspace id under sourcedata/ws
    retrieve_sample - whether to retrieve sample for each genome object
    ignore_no_sample_error - whether to ignore the error when no sample data is found
    ws - workspace client
    asu - assemblyUtil client
    ss - sampleService client
    pools - a pool of worker processes

    """
    def __init__(
        self,
        job_dir: str,
        output_dir: str | None = None,
        kb_base_url: str = "https://ci.kbase.us/services/",
        token_filepath: str | None = None,
        au_service_ver: str = "release",
        workers: int = 5,
        max_callback_server_tasks: int = 20,
        worker_function: Callable | None = None,
        retrieve_sample: bool = False,
        ignore_no_sample_error: bool = False,
        workspace_downloader: bool = False,
        catalog_admin: bool = False,
    ) -> None:
        """
        Initialize the configuration class.

        Args:
            job_dir (str): The directory for SDK jobs per user.
            output_dir (str): The directory for a specific workspace id under sourcedata/ws.
            kb_base_url (str): The base url of the KBase services.
            token_filepath (str): The file path that stores a KBase token appropriate for the KBase environment.
                                If not supplied, the token must be provided in the environment variable KB_AUTH_TOKEN.
                                The KB_ADMIN_AUTH_TOKEN environment variable will get set by this token if the user runs as catalog admin.
            au_service_ver (str): The service version of AssemblyUtilClient
                                ('dev', 'beta', 'release', or a git commit).
            workers (int): The number of workers to use for multiprocessing.
            max_callback_server_tasks (int): The maximum number of subtasks for the callback server.
            worker_function (Callable): The function that will be called by the workers.
            retrieve_sample (bool): Whether to retrieve sample for each genome object.
            ignore_no_sample_error (bool): Whether to ignore the error when no sample data is found.
            workspace_downloader (bool): Whether to be used for the workspace downloader script.
            catalog_admin (bool): Whether to run the callback server as catalog admin.
        """
        ipv4 = loader_helper.get_ip()
        port = loader_helper.find_free_port()

        # common instance variables

        self.token = loader_helper.get_token(token_filepath)

        # setup and run callback server container
        self._start_callback_server(
            docker.from_env(),
            uuid.uuid4().hex,
            job_dir,
            kb_base_url,
            self.token,
            port,
            max_callback_server_tasks,
            ipv4,
            catalog_admin,
        )

        self.callback_url = "http://" + ipv4 + ":" + str(port)
        print("callback_url:", self.callback_url)

        self.job_data_dir = loader_helper.make_job_data_dir(job_dir)

        # unique to downloader
        if workspace_downloader:
            if worker_function is None:
                raise ValueError(
                    "worker_function cannot be None for the workspace downloader script"
                )

            self.input_queue = Queue()
            self.output_dir = output_dir

            self.retrieve_sample = retrieve_sample
            self.ignore_no_sample_error = ignore_no_sample_error

            ws_url = os.path.join(kb_base_url, "ws")
            self.ws = Workspace(ws_url, token=self.token)

            self.asu = AssemblyUtil(
                self.callback_url, service_ver=au_service_ver, token=self.token
            )

            sample_url = os.path.join(kb_base_url, "sampleservice")
            self.ss = SampleService(sample_url, token=self.token)

            self.pools = Pool(workers, worker_function, [self])

    def _setup_callback_server_envs(
        self,
        job_dir: str,
        kb_base_url: str,
        token: str,
        port: int,
        max_callback_server_tasks: int,
        ipv4: str,
        catalog_admin: bool,
    ) -> Tuple[dict[str, Union[int, str]], dict[str, dict[str, str]]]:
        """
        Setup the environment variables and volumes for the callback server.

        Args:
            job_dir (str): The directory for SDK jobs per user.
            kb_base_url (str): The base url of the KBase services.
            token (str): The KBase token.
            port (int): The port number for the callback server.
            max_callback_server_tasks (int): The maximum number of subtasks for the callback server.
            ipv4 (str): The ipv4 address for the callback server.
            catalog_admin (bool): Whether to run the callback server as catalog admin.

        Returns:
            tuple: A tuple of the environment variables and volumes for the callback server.
        """
        # initiate env and vol
        env = {}
        vol = {}

        # used by the callback server
        env["KB_AUTH_TOKEN"] = token
        env["KB_BASE_URL"] = kb_base_url
        env["JOB_DIR"] = job_dir
        env["CALLBACK_PORT"] = port
        env["JR_MAX_TASKS"] = max_callback_server_tasks
        env["CALLBACK_IP"] = ipv4  # specify an ipv4 address for the callback server
                                   # otherwise, the callback container will use the an ipv6 address

        # set admin token to get catalog secure params
        if catalog_admin:
            env["KB_ADMIN_AUTH_TOKEN"] = token

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
        max_callback_server_tasks: int,
        ipv4: str,
        catalog_admin: bool,
    ) -> None:
        """
        Start the callback server.

        Args:
            client (docker.client): The docker client.
            container_name (str): The name of the container.
            job_dir (str): The directory for SDK jobs per user.
            kb_base_url (str): The base url of the KBase services.
            token (str): The KBase token.
            max_callback_server_tasks (int): The maximum number of subtasks for the callback server.
            port (int): The port number for the callback server.
            ipv4 (str): The ipv4 address for the callback server.
            catalog_admin (bool): Whether to run the callback server as catalog admin.
        """
        env, vol = self._setup_callback_server_envs(
            job_dir,
            kb_base_url,
            token,
            port,
            max_callback_server_tasks,
            ipv4,
            catalog_admin,
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
