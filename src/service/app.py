'''
API for the collections service.
'''
import os

from src.common.git_commit import GIT_COMMIT
from src.common.version import VERSION
from datetime import datetime, timezone
from fastapi import FastAPI, Depends, Request
from fastapi.security.http import HTTPBearer, HTTPBasicCredentials
from pydantic import BaseModel, Field

from src.service.kb_auth import create_auth_client, AdminPermission


# TODO ERROR_HANDLING - create a general error structure and convert exceptions to it
# TODO LOGGING - log all write ops


# TODO CONFIG these should be configured vs hard coded
AUTH2_ADMIN_ROLES = ["KBASE_ADMIN", "COLLECTIONS_SERVICE_ADMIN"]
CI_AUTH_URL = "https://ci.kbase.us/services/auth"

SERVICE_NAME = "Collections Prototype"

app_config = {
    "title": SERVICE_NAME,
    "description": "A repository of data collections and and associated analyses",
    "version": VERSION,
}


# TODO CONFIG rethink how to do this. Config file? How to test? Maybe manually is the only
# reasonable option
_FASTAPI_ROOT_PATH = "FASTAPI_ROOT_PATH"
if os.environ.get(_FASTAPI_ROOT_PATH):
    app_config.update({
        "root_path": os.environ[_FASTAPI_ROOT_PATH],
    })


app = FastAPI(**app_config)
authheader = HTTPBearer()


@app.on_event('startup')
async def build():
    app.state.kb_auth = await create_auth_client(CI_AUTH_URL, AUTH2_ADMIN_ROLES)


async def get_user(creds:HTTPBasicCredentials):
    # TODO ERROR_HANDLING handle invalid token
    return await app.state.kb_auth.get_user(creds.credentials)


def timestamp():
    return datetime.now(timezone.utc).isoformat()


class Root(BaseModel):
    service_name: str = Field(example=SERVICE_NAME)
    version: str = Field(example=VERSION)
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")
    server_time: str = Field(example="2022-10-07T17:58:53.188698+00:00")


class WhoAmI(BaseModel):
    user: str = Field(example="kbasehelp")
    is_service_admin: bool = Field(example=False)


@app.get("/", response_model=Root)
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }


@app.get("/whoami", response_model = WhoAmI)
async def whoami(creds: HTTPBasicCredentials=Depends(authheader)):
    admin, user = await get_user(creds)
    return {"user": user.id, "is_service_admin": AdminPermission.FULL == admin}

