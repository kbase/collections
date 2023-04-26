"""
Functions for creating and handling application state.

All functions assume that the application state has been appropriately initialized via
calling the build_app() method
"""

import asyncio

from fastapi import FastAPI, Request
from src.service._app_state_build_storage import build_storage
from src.service.app_state_data_structures import CollectionsState
from src.service.config import CollectionsServiceConfig
from src.service.deletion import SubsetCleanup
from src.service.data_products.common_models import DataProductSpec
from src.service.kb_auth import KBaseAuth
from src.service.matchers.common_models import Matcher
from src.service.sdk_async_client import SDKAsyncClient

# The main point of this module is to handle all the application state in one place
# to keep it consistent and allow for refactoring without breaking other code


async def build_app(
    app: FastAPI,
    cfg: CollectionsServiceConfig,
    data_products: list[DataProductSpec],
    matchers: list[Matcher],
) -> None:
    """
    Build the application state.

    app - the FastAPI app.
    cfg - the collections service config.
    data_products - the data products installed in the system
    matchers - the matchers installed in the system
    """
    auth = await KBaseAuth.create(cfg.auth_url, cfg.auth_full_admin_roles)
    sdk_client = SDKAsyncClient(cfg.workspace_url)
    cli = None
    try:
        cli, storage = await build_storage(cfg, data_products)
        await _check_workspace_url(sdk_client, cfg.workspace_url)
        app.state._colstate = CollectionsState(
            auth, sdk_client, cli, storage, matchers, cfg
        )
        app.state._match_deletion = SubsetCleanup(
            app.state._colstate.get_pickleable_dependencies(),
            interval_sec=1 * 24 * 60 * 60,
            jitter_sec=60 * 60,
            subset_age_ms=7 * 24 * 60 * 60 * 1000
            )
        app.state._match_deletion.start()
    except Exception as e:
        if cli:
            await cli.close()
        await sdk_client.close()
        raise e


def get_app_state(r: Request) -> CollectionsState:
    """
    Get the application state from a request.
    """
    return _get_app_state_from_app(r.app)


async def destroy_app_state(app: FastAPI):
    """
    Destroy the application state, shutting down services and releasing resources.
    """
    colstate = _get_app_state_from_app(app)  # first to check state was set up
    app.state._match_deletion.stop()
    await colstate.destroy()
    # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
    await asyncio.sleep(0.250)


def _get_app_state_from_app(app: FastAPI) -> CollectionsState:
    if not app.state._colstate:
        raise ValueError("App state has not been initialized")
    return app.state._colstate


async def _check_workspace_url(sdk_cli: SDKAsyncClient, ws_url: str) -> str:
    try:
        ver = await sdk_cli.call("Workspace.ver")
        # could check the version later if we add dependencies on newer versions
        print("Workspace version: " + ver)
    except Exception as e:
        raise ValueError(f"Could not connect to workspace at {ws_url}: {str(e)}") from e
