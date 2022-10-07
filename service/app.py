'''
API for the collections service.
'''

from fastapi import FastAPI
from common.version import VERSION
from common.git_commit import GIT_COMMIT
from pydantic import BaseModel, Field


class Root(BaseModel):
    service_name: str = Field(example="Collections")
    version: str = Field(example="0.4.6")
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")


app = FastAPI()


@app.get("/", response_model=Root)
async def root():
    return {
        "service_name": "Collections",
        "version": VERSION,
        "git_hash": GIT_COMMIT
    }