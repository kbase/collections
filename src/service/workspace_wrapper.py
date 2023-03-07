"""
Wrapper for a workspace client that provides a simplified interface for the needs of the
collections service.
"""

from typing import Any
from src.service import errors
from src.service.clients.workspace_client import Workspace
from src.service.clients.baseclient import ServerError

WORKSPACE_UPA_PATH = "__workspace_upa_path__"
"Field added to workspace metadata containing the path to the object."


class WorkspaceWrapper:
    """ A wrapper for a workspace client for the collections service. """

    def __init__(self, workspace: Workspace):
        """
        Create the wrapper.

        workspace - a workspace instance, initialized with appropriate credientials.
        """
        self._ws = workspace

    def _get_type(self, obj_info) -> str:
        return obj_info[2].split('-')[0]

    def get_object_metadata(
        self,
        upas: list[str],
        allowed_types: set[str] | None = None,
        allowed_set_types: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        f"""
        Get metadata for selected workspace objects.

        upas - the UPAs of objects from which to retrieve metadata.
        allowed_types - the allowed types of the objects. If object types are not in this list,
            an error will be thrown.
        allowed_set_types - allowed types of object sets. The object sets are expanded by assuming
            all their references are to target objects (e.g. reference context in the object is
            not taken info account), and those objects are checked against allowed_types if
            provided.

        Returns a list of the metadata, adding a special field, {WORKSPACE_UPA_PATH},
        to the metadata, with the path to the workspace object in string format, e.g. 3/2/1;6/5/4.
        """
        # The workspace error format really sucks. Error codes are bascially a necessity for
        # handling errors reasonably - trying to figure out the error type from an arbitrary
        # string is not reasonable
        std_objs, set_objs = self._get_object_info(upas, allowed_types, allowed_set_types)
        std_objs2 = []
        if set_objs:
            paths = [meta[WORKSPACE_UPA_PATH] for meta in set_objs]
            res = self._ws.get_objects2(
                {"objects": [{'ref': p} for p in paths], "no_data": 1, 'ignoreErrors': 1}
            )['data']
            set_upas = set()
            for path, o in zip(paths, res):
                if not o:
                    # Race condition - the workspace allowed get_info3 but denied on get_objects2
                    # Need to test with mocked workspace client
                    raise errors.DataPermissionError(
                        f"The workspace service disallowed acces to object {path}"
                    )
                set_upas.update([f'{path};{r}' for r in o['refs']])
            if len(set_upas) > 10000:
                # TODO HUGE_INPUT may want to loop through 10k at a time?
                raise errors.IllegalParameterError(
                        f"There are more than 10000 objects in the combined object sets"
                    )
            std_objs2, _ = self._get_object_info(set_upas, allowed_types, set())
        return std_objs + std_objs2

    def _get_object_info(self, upas, allowed_types, allowed_set_types):
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
        set_objs = []
        std_objs = []
        for info, path, upa in zip(res['infos'], res['paths'], upas):
            if not info:
                raise errors.DataPermissionError(
                    f"The workspace service disallowed access to object {upa}")
            info[10][WORKSPACE_UPA_PATH] = ";".join(path)
            type_ = self._get_type(info)
            if allowed_set_types and type_ in allowed_set_types:
                set_objs.append(info[10])
            elif allowed_types and type_ not in allowed_types:
                raise errors.IllegalParameterError(
                    f"Workspace object {upa} is type {info[2]}, which is not one of "
                    + f"the allowed types: {allowed_types}"
                )
            else:
                std_objs.append(info[10])
        return std_objs, set_objs

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
