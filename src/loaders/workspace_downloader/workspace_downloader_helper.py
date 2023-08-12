import os
import time
import uuid
from multiprocessing import Pool, Queue
from typing import Callable

import docker

from src.clients.AssemblyUtilClient import AssemblyUtil
from src.clients.SampleServiceClient import SampleService
from src.clients.workspaceClient import Workspace
from src.loaders.common import loader_helper
from src.loaders.common.loader_common_names import CALLBACK_UPLOADER_IMAGE_NAME


class Conf:
    def __init__(
            self,
            job_dir: str,
            output_dir: str,
            worker_function: Callable,
            kb_base_url: str = "https://ci.kbase.us/services/",
            token_filepath: str | None = None,
            workers: int = 5,
            retrieve_sample: bool = False,
            ignore_no_sample_error: bool = False,
    ):
        port = loader_helper.find_free_port()
        token = loader_helper.get_token(token_filepath)
        self.retrieve_sample = retrieve_sample
        self.ignore_no_sample_error = ignore_no_sample_error
        self.start_callback_server(
            docker.from_env(),
            uuid.uuid4().hex,
            job_dir,
            kb_base_url,
            token,
            port,
        )

        ws_url = os.path.join(kb_base_url, "ws")
        sample_url = os.path.join(kb_base_url, "sampleservice")
        callback_url = "http://" + loader_helper.get_ip() + ":" + str(port)
        print("callback_url:", callback_url)

        self.ws = Workspace(ws_url, token=token)
        self.asu = AssemblyUtil(callback_url, token=token)
        self.ss = SampleService(sample_url, token=token)

        self.workers = workers
        self.output_dir = output_dir
        self.job_dir = job_dir
        self.input_queue = Queue()
        self.output_queue = Queue()
        self.pools = Pool(workers, worker_function, [self])

    def setup_callback_server_envs(self, job_dir, kb_base_url, token, port):
        # initiate env and vol
        env = {}
        vol = {}

        # used by the callback server
        env["KB_AUTH_TOKEN"] = token
        env["KB_BASE_URL"] = kb_base_url
        env["JOB_DIR"] = job_dir
        env["CALLBACK_PORT"] = port

        # setup volumes required for docker container
        docker_host = os.environ["DOCKER_HOST"]
        if docker_host.startswith("unix:"):
            docker_host = docker_host[5:]

        vol[job_dir] = {"bind": job_dir, "mode": "rw"}
        vol[docker_host] = {"bind": "/run/docker.sock", "mode": "rw"}

        return env, vol

    def start_callback_server(
            self, client, container_name, job_dir, kb_base_url, token, port
        ):
        env, vol = self.setup_callback_server_envs(job_dir, kb_base_url, token, port)
        self.container = client.containers.run(
            name=container_name,
            image=CALLBACK_UPLOADER_IMAGE_NAME,
            detach=True,
            network_mode="host",
            environment=env,
            volumes=vol,
        )
        time.sleep(2)

    def stop_callback_server(self):
        self.container.stop()
        self.container.remove()