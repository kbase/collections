'''
API for the collections service.
'''

from fastapi import FastAPI
from common.version import VERSION
from common.git_commit import GIT_COMMIT

app = FastAPI()


@app.get("/")
async def root():
    return {
        "service": "Collections",
        "version": VERSION,
        "git_hash": GIT_COMMIT
    }