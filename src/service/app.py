'''
API for the collections service.
'''
import os

from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from datetime import datetime, timezone
from fastapi import FastAPI
from pydantic import BaseModel, Field


SERVICE_NAME = "Collections Prototype"

app_config = {
    "title": SERVICE_NAME,
    "description": "A repository of data collections and and associated analyses",
    "version": VERSION,
}


# TODO rethink how to do this. Config file? How to test? Maybe manually is the only reasonable
# option
_FASTAPI_ROOT_PATH = "FASTAPI_ROOT_PATH"
if os.environ.get(_FASTAPI_ROOT_PATH):
    app_config.update({
        "root_path": os.environ[_FASTAPI_ROOT_PATH],
    })


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class Root(BaseModel):
    service_name: str = Field(example=SERVICE_NAME)
    version: str = Field(example=VERSION)
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")
    server_time: str = Field(example="2022-10-07T17:58:53.188698+00:00")


app = FastAPI(**app_config)


@app.get("/", response_model=Root)
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }