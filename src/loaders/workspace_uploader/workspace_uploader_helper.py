import os
import time
import uuid
from typing import Tuple, Union

import docker

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_helper
from src.loaders.common.loader_common_names import CALLBACK_IMAGE_NAME


class Conf:
    """
    Configuration class for the workspace uploader script.
    """
    def __init__(
            self,
            job_dir: str,
            output_dir: str,
            kb_base_url: str = "https://ci.kbase.us/services/",
            token_filepath: str | None = None,
            au_service_ver: str = "release",
    ) -> None:
        """
        Initialize the configuration class.

        Args:
            job_dir (str): The directory for SDK jobs per user.
            output_dir (str): The directory for a specific workspace id under sourcedata/ws.
            kb_base_url (str): The base url of the KBase services.
            token_filepath (str): The file path that stores a KBase token appropriate for the KBase environment. 
                                If not supplied, the token must be provided in the environment variable KB_AUTH_TOKEN.
            au_service_ver (str): The service verison of AssemblyUtilClient.
        """
        port = loader_helper.find_free_port()
        token = loader_helper.get_token(token_filepath)
        ipv4 = loader_helper.get_ip()
        self._start_callback_server(
            docker.from_env(),
            uuid.uuid4().hex,
            job_dir,
            kb_base_url,
            token,
            port,
            ipv4,
        )

        ws_url = os.path.join(kb_base_url, "ws")
        callback_url = "http://" + ipv4 + ":" + str(port)
        print("callback_url:", callback_url)

        self.ws = Workspace(ws_url, token=token)
        self.asu = AssemblyUtil(callback_url, service_ver=au_service_ver, token=token)

        self.output_dir = output_dir
        self.job_data_dir = loader_helper.make_job_data_dir(job_dir)

    def _setup_callback_server_envs(
            self,
            job_dir: str,
            kb_base_url: str,
            token: str,
            port: int,
            ipv4: str,
    ) -> Tuple[dict[str, Union[int, str]], dict[str, dict[str, str]]]:
        """
        Setup the environment variables and volumes for the callback server.

        Args:
            job_dir (str): The directory for SDK jobs per user.
            kb_base_url (str): The base url of the KBase services.
            token (str): The KBase token.
            port (int): The port number for the callback server.

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
            port (int): The port number for the callback server.
        """
        env, vol = self._setup_callback_server_envs(job_dir, kb_base_url, token, port, ipv4)
        self.container = client.containers.run(
            name=container_name,
            image=CALLBACK_IMAGE_NAME,
            detach=True,
            network_mode="host",
            environment=env,
            volumes=vol,
        )
        time.sleep(2)

    def stop_callback_server(self) -> None:
        """
        Stop the callback server.
        """
        self.container.stop()
        self.container.remove()
