'''
API for the collections service.
'''
import os

from common.git_commit import GIT_COMMIT
from common.version import VERSION
from datetime import datetime, timezone
from fastapi import FastAPI
from pydantic import BaseModel, Field


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class Root(BaseModel):
    service_name: str = Field(example="Collections")
    version: str = Field(example="0.4.6")
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")
    server_time: str = Field(example="2022-10-07T17:58:53.188698+00:00")


# TODO rethink how to do this. Config file? How to test? Maybe manually is the only reasonable
# option
if os.environ.get("FASTAPI_ROOT_PATH"):
    app = FastAPI(root_path=os.environ["FASTAPI_ROOT_PATH"])
else:
    app = FastAPI()


@app.get("/", response_model=Root)
async def root():
    return {
        "service_name": "Collections",
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }