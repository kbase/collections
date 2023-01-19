"""
Wrapper for a workspace client that provides a simplified interface for the needs of the
collections service.
"""

from typing import Any
from src.service import errors
from src.service.clients.workspace_client import Workspace
from src.service.clients.baseclient import ServerError


class WorkspaceWrapper:
    """ A wrapper for a workspace client for the collections service. """

    def __init__(self, workspace: Workspace):
        """
        Create the wrapper.

        workspace - a workspace instance, initialized with appropriate credientials.
        """
        self._ws = workspace

    def get_object_metadata(
        self,
        upas: list[str],
        allowed_types: set[str] | None = None
    ) -> list[dict[str, Any]]:
        """
        Get metadata for selected workspace objects.

        upas - the UPAs of objects from which to retrieve metadata.
        allowed_types - the allowed types of the objects. If object types are not in this list,
            an error will be thrown.

        Returns a list of the metadata in the same order as the UPAs.
        """
        # The workspace error format really sucks. Error codes are bascially a necessity for
        # handling errors reasonably - trying to figure out the error type from an arbitrary
        # string is not reasonable
        refs = [{"ref": u} for u in upas]
        # just throw any ws errors here. Could add retries... later
        # Without doing string inspection of the error message it's impossible to tell if
        # the error is due to user error (e.g. malformed input) or something else (e.g. ws 
        # lost connection to mongo). As such just throw and it'll wind up as a 500
        res = self._ws.get_object_info3({
            "objects": refs,
            "ignoreErrors": 1,
            "includeMetadata": 1,
        })
        for info, upa in zip(res['infos'], upas):
            if not info:
                raise errors.DataPermissionError(
                    f"The workspace service disallowed access to object {upa}")
            if allowed_types and info[2].split('-')[0] not in allowed_types:
                raise errors.IllegalParameterError(
                    f"Workspace object {upa} is type {info[2]}, which is not one of "
                    + f"the allowed types: {allowed_types}"
                )
        return [info[10] for info in res['infos']]

    def check_workspace_permissions(self, workspace_ids: set[int]) -> None:
        """
        Check that the credentials in the workspace client have access to the provided workspaces.
        If not, an error will be thrown.
        """
        # A mass get_workspace_info method with reasonable error codes here would help a lot
        # As it is this is pretty fragile and any user input errors are probably going to just
        # get re thrown. The comments on the get_lineages function are relevant here as well
        for wsi in workspace_ids:
            try:
                self._ws.get_workspace_info({"id": wsi})
            except ServerError as e:
                if any(x in e.message for x in ["may not read workspace", "No workspace with id"]):
                    raise errors.DataPermissionError(
                        f"The workspace service disallowed access to workspace {wsi}") from e
                # Could be lots of other causes here, and the only way to tell is to inspect
                # the workspace source code for error strings, so...
                raise e
