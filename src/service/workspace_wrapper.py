"""
Wrapper for a workspace client that provides a simplified interface for the needs of the
collections service.
"""

from typing import Any, Iterable, Annotated
from pydantic import BaseModel, Field
from src.service import errors
from src.service.sdk_async_client import SDKAsyncClient, ServerError


WORKSPACE_UPA_PATH = "__workspace_upa_path__"
"Field added to workspace metadata containing the path to the object."


_TYPE_TO_SET_INFO = {
    "KBaseGenomes.Genome": {
        # eventually supposed to be replaced by the KBaseSets version which has the same structure
        # as assemblies
        "type": "KBaseSearch.GenomeSet",
        # Not documented what the key here is supposed to be, just matters that it's unique
        # per Dylan. He uses the UPA, so so will I
        "items": lambda upas: {"elements": {u: {"ref": u} for u in upas}}
    },
    "KBaseGenomeAnnotations.Assembly": {
        "type": "KBaseSets.AssemblySet",
        "items": lambda upas: {"items": [{"ref": u} for u in upas]}
    },
}


class SetSpec(BaseModel):
    """
    Information required to save a set.
    """
    name: str = Field(description="The target object name of the set.")
    upas: list[str] = Field(description="The UPAs of the objects making up the set")
    upa_type: str = Field(description="The type of the objects in the UPA list")
    description: str | None = Field(description="A description of the set")
    provenance: Annotated[dict[str, Any], Field(
        description="The workspace provenance action for the set, if any. "
            + "Multiple provenance actions are not supported."
    )] = None 


class WorkspaceWrapper:
    """
    A wrapper for a workspace client for the collections service.
    
    Instance Variables:
    token - the user's token, if any.
    """

    def __init__(self, sdk_cli: SDKAsyncClient, token: str = None):
        """
        Create the wrapper.

        sdk_cli - an SDK client instance.
        token - a user's KBase token, if any.
        """
        self._cli = sdk_cli
        self.token = token

    def _get_type(self, obj_info) -> str:
        return obj_info[2].split('-')[0]

    async def get_object_metadata(
        self,
        upas: list[str],
        allowed_types: Iterable[str] | None = None,
        allowed_set_types: Iterable[str] | None = None,
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
        # The workspace error format really sucks. Error codes are basically a necessity for
        # handling errors reasonably - trying to figure out the error type from an arbitrary
        # string is not reasonable
        allowed_types = set(allowed_types) if allowed_types else {}
        allowed_set_types = set(allowed_set_types) if allowed_set_types else {}
        std_objs, set_objs = await self._get_object_info(
            upas, allowed_types, allowed_set_types)
        std_objs2 = []
        if set_objs:
            paths = [meta[WORKSPACE_UPA_PATH] for meta in set_objs]
            res = await self._cli.call(
                "Workspace.get_objects2", 
                [{"objects": [{'ref': p} for p in paths], "no_data": 1, 'ignoreErrors': 1}],
                token=self.token,
            )
            res = res['data']
            set_upas = set()
            for path, o in zip(paths, res):
                if not o:
                    # Race condition - the workspace allowed get_info3 but denied on get_objects2
                    # Need to test with mocked workspace client
                    raise errors.DataPermissionError(
                        f"The workspace service disallowed access to object {path}"
                    )
                set_upas.update([f'{path};{r}' for r in o['refs']])
            if len(set_upas) > 10000:
                # TODO HUGE_INPUT may want to loop through 10k at a time?
                raise errors.IllegalParameterError(
                        f"There are more than 10000 objects in the combined object sets"
                    )
            std_objs2, _ = await self._get_object_info(set_upas, allowed_types, set())
        return std_objs + std_objs2

    async def _get_object_info(self, upas, allowed_types, allowed_set_types):
        refs = [{"ref": u} for u in upas]
        # just throw any ws errors here. Could add retries... later
        # Without doing string inspection of the error message it's impossible to tell if
        # the error is due to user error (e.g. malformed input) or something else (e.g. ws 
        # lost connection to mongo). As such just throw and it'll wind up as a 500
        res = await self._cli.call(
            "Workspace.get_object_info3",
            [{ "objects": refs, "ignoreErrors": 1, "includeMetadata": 1}],
            token=self.token,
        )
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
                    + f"the allowed types: {sorted(allowed_types | allowed_set_types)}"
                )
            else:
                std_objs.append(info[10])
        return std_objs, set_objs

    async def check_workspace_permissions(self, workspace_ids: set[int]) -> None:
        """
        Check that the user has access to the provided workspaces.
        If not, an error will be thrown.
        """
        # A mass get_workspace_info method with reasonable error codes here would help a lot
        # As it is this is pretty fragile and any user input errors are probably going to just
        # get re thrown. The comments on the get_lineages function are relevant here as well
        for wsi in workspace_ids:
            try:
                await self._cli.call(
                    "Workspace.get_workspace_info", [{"id": wsi}], token=self.token
                )
            except ServerError as e:
                self._check_err(wsi, e)

    def _check_err(self, wsid: int, e: ServerError):
        if "has invalid reference" in e.message:
            # error message is sort of gross but good enough for now, since this should never
            # happen for collections, since the target upas are public and we're setting up
            # the data types and don't expect type mismatch errors from the set @ws annotations
            raise errors.DataPermissionError(e.message)
        if any(x in e.message for x in 
            ["may not read workspace", "No workspace with id", "may not write"]
        ):
            raise errors.DataPermissionError(
                f"The workspace service disallowed access to workspace {wsid}") from e
        # Could be lots of other causes here, and the only way to tell is to inspect
        # the workspace source code for error strings, so...
        raise e

    def _obj_info_to_upa(self, objinfo) -> str:
        return f"{objinfo[6]}/{objinfo[0]}/{objinfo[4]}"

    async def save_sets(self, wsid: int, sets: list[SetSpec]) -> dict[str, str]:
        """
        Save one or more sets to the workspace.

        wsid - the ID of the workspace to save to.
        sets - the sets to save.

        Returns a mapping of the resulting set UPAs to their type.
        """
        # should add some size checks in future, like max # of sets, max # of items in set, etc
        if not sets:
            raise ValueError("no sets")
        objs = []
        for s in sets:
            if not s.upas:
                raise ValueError("all sets must have at least one UPA")
            setinfo = _TYPE_TO_SET_INFO.get(s.upa_type)
            if not setinfo:
                raise errors.IllegalParameterError(f"Unsupported workspace type: {s.upa_type}")
            wsset = {"description": s.description} | setinfo["items"](s.upas)
            objs.append({
                "name": s.name,
                "type": setinfo["type"],
                "data": wsset,
                # might want to do some error checking here or make a provenance data structure...
                # YAGNI for now
                "provenance": [s.provenance],
            })
        try:
            res = await self._cli.call(
                "Workspace.save_objects",
                [{"id": wsid, "objects": objs}],
                token=self.token,
            )
            return {self._obj_info_to_upa(o): o[2] for o in res}
        except ServerError as e:
            self._check_err(wsid, e)
