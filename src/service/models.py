"""
Global models for the service that cross API and storage boundaries. These models define
both

* The structure and keys accepted by and returned to the API
* The structure and keys accepted by and returned from the storage systems.

As such, changes must be made very carefully. For instance, a simple key change may mean
that older data in the database will no longer be fetched correctly - but automated tests
may still pass. In this case, a translation from the older data is required.
"""

from enum import Enum
from pydantic import field_validator, BaseModel, Field, HttpUrl, model_validator
from typing import Any, Self, Annotated

from src.service.arg_checkers import contains_control_characters
from src.service.filtering import generic_view

# TODO TEST all these regexes and constraints will need a good chunk of testing.

REGEX_NO_WHITESPACE = "^[^\\s]+$"
REGEX_LOAD_VERSION = r"^[\w.-]+$"
LENGTH_MIN_LOAD_VERSION = 1
LENGTH_MAX_LOAD_VERSION = 20

# regarding control character handling - you can check for control characters with the regex
# \pC escape - BUT Pydantic uses the built in re library which doesn't support it. The
# 3rd party regex library does, but that means you need to make a custom validator (like the
# ones below). If that's the case, might was well just use the control character checker
# we already have and which is pretty simple, and which can be easily customized to allow
# other control characters if needed (not likely).


# Model fields for use elsewhere
# If the model fields change, they must be changed here as well or things might break
# Is there a better way to do this? Seems absurdly hacky
FIELD_COLLECTION_ID = "id"
FIELD_VER_TAG = "ver_tag"
FIELD_VER_NUM = "ver_num"
FIELD_COLLSPEC_COLLECTION_ID = "collection_id"
FIELD_COLLSPEC_COLLECTION_VER = "collection_ver"
FIELD_DATA_PRODUCTS = "data_products"
FIELD_DATA_PRODUCTS_PRODUCT = "product"
FIELD_MATCHERS = "matchers"
FIELD_MATCHERS_MATCHER = "matcher"
FIELD_MATCH_INTERNAL_MATCH_ID = "internal_match_id"
FIELD_MATCH_USER_PERMS = "user_last_perm_check"
FIELD_MATCH_MATCHES = "matches"
FIELD_MATCH_MATCH_COUNT = "match_count"
FIELD_MATCH_WSIDS = "wsids"
FIELD_SELECTION_INTERNAL_SELECTION_ID = "internal_selection_id"
FIELD_SELECTION_UNMATCHED_IDS = "unmatched_ids"
FIELD_SELECTION_UNMATCHED_COUNT = "unmatched_count"
FIELD_DATA_PRODUCT_PROCESS_MISSING_IDS = "missing_ids"
FIELD_DATE_CREATE = "date_create"
FIELD_USER_CREATE = "user_create"
FIELD_DATE_ACTIVE = "date_active"
FIELD_USER_ACTIVE = "user_active"
# These 4 fields apply to both selections and matches
FIELD_PROCESS_HEARTBEAT = "heartbeat"
FIELD_PROCESS_STATE = "state"
FIELD_PROCESS_STATE_UPDATED = "state_updated"
FIELD_LAST_ACCESS = "last_access"
# data product process exclusive fields
FIELD_PROCESS_TYPE = "type"

# Model metadata for use elsewhere
FIELD_COLLECTION_ID_EXAMPLE = "GTDB"
FIELD_COLLECTION_ID_DESCRIPTION = "The unique ID of the collection."
FIELD_VER_TAG_EXAMPLE = "r207.kbase.2"
FIELD_VER_TAG_DESCRIPTION = ("A user assigned unique but otherwise arbitrary tag for the "
    + "collection version.")
FIELD_VER_NUM_EXAMPLE = 5
FIELD_VER_NUM_DESCRIPTION = ("The numeric version of the collection, assigned by the "
    + "collection service")
FIELD_LOAD_VERSION_EXAMPLE = "gtdb.207.kbase.3"
FIELD_LOAD_VERSION_DESCRIPTION = "The load version of the data product"
FIELD_UPA_LIST_EXAMPLE = ["75/23/1", "100002/106/3;7/54/9"]
FIELD_UPA_LIST_DESCRIPTION= (
"""
The Unique Permanent Addresses (UPAs) for the input workspace objects
that are being matched against the collection. UPAs are in the format
W/O/V, where W is the workspace integer ID, O is the integer object ID, and
V is the version. The version may not be omitted. Reference paths
(e.g. a sequence of UPAs separated by ';') are allowed.
"""
)
FIELD_USER_PARAMETERS_EXAMPLE = {"gtdb_rank": "species"}
FIELD_USER_PARAMETERS_DESCRIPTION = "The user parameters for the match."
FIELD_MATCHER_PARAMETERS_EXAMPLE = {'gtdb_version': '207.0'}
FIELD_MATCHER_PARAMETERS_DESCRIPTION = ("Any collection (as opposed to user provided) parameters "
    + "for the matcher. What these are will depend on the matcher in question")
FIELD_SELECTION_EXAMPLE = ["GB_GCA_000006155.2", "GB_GCA_000007385.1"]
FIELD_SELECTION_IDS_DESCRIPTION = (
    "The IDs of the selected items. What these IDs are will depend on the " +
    "collection and data product the selection is against."
)
FIELD_SELECTION_UNMATCHED_DESCRIPTION = (
    "IDs of the selected items that were not found in the data."
)


# this seems stupid...
DATA_PRODUCT_ID_FIELD_PROPS = {
    "min_length": 1,
    "max_length": 20,
    "pattern": "^[a-z_]+$",
    "example": "taxa_count",
    "description": "The ID of the data product",
}


DATA_PRODUCT_ID_FIELD = Field(**DATA_PRODUCT_ID_FIELD_PROPS)


MATCHER_ID_PROPS = {
    "min_length": 1,
    "max_length": 20,
    "pattern": "^[a-z_]+$",
    "example": "gtdb_lineage",
    "description": "The ID of the matcher",
}


MATCHER_ID_FIELD = Field(**MATCHER_ID_PROPS)


class DynamicConfig(BaseModel):
    """
    Holds dynamic configuration data for the service.
    Currently editable by editing the database directly.
    """
    search_views: Annotated[dict[str, str], Field(
        example={"genome_attribs": "gaview_commithash"},
        description="A mapping of data product -> ArangoSearch view name. "
            + "When a particular data product requires a view for one of its collections "
            + "this mapping specifies the name of the view to use. This variable can be used to "
            + "seamlessly switch between an old and updated view by changing the variable value "
            + "after the new view is built."
         
    )] = {}
    
    def is_empty(self):
        return not self.search_views


class DataProduct(BaseModel):
    """The ID and version of a data product associated with a collection"""
    product: str = DATA_PRODUCT_ID_FIELD
    version: str = Field(
        min_length = LENGTH_MIN_LOAD_VERSION,
        max_length = LENGTH_MAX_LOAD_VERSION,
        pattern = REGEX_LOAD_VERSION,
        example=FIELD_LOAD_VERSION_EXAMPLE,
        description=FIELD_LOAD_VERSION_DESCRIPTION
    )
    search_view: Annotated[str | None, Field(
        example="collection_service_genome_attribs_v1.5.2",
        description="The name of the ArangoSearch view to use for searches for this data "
            + "product. The view must exist on collection creation. Many data products don't "
            + "support search and therefore do not need this configured."
    )] = None
    # in the future we may want a schema version... need to think this through first.
    # assume missing == schema version 1 for now

    @field_validator("product", "version", mode="before")
    @classmethod
    def _strip(cls, v):
        return v.strip()


class Matcher(BaseModel):
    """The ID of a matcher associated with a collection and any parameters for the matcher"""
    matcher: str = MATCHER_ID_FIELD
    parameters: dict[str, Any] = Field(
        example=FIELD_MATCHER_PARAMETERS_EXAMPLE,
        description=FIELD_MATCHER_PARAMETERS_DESCRIPTION,
    )


class Collection(BaseModel):
    """
    A collection document provided to the collections service to save.
    """
    name: str = Field(
        min_length=1,
        max_length=50,
        example="Genome Taxonomy Database",
        description="The name of the collection."
    )
    ver_src: str = Field(
        min_length=1,
        max_length=50,
        pattern=REGEX_NO_WHITESPACE,
        example="r207",
        description="The version of the collection at the collection data source."
    )
    desc: Annotated[
        str | None,
        Field(
            min_length=1,
            max_length=1000,
            example="This is a collection of used hot dogs collected from Coney Island in 1892.",
            description="A free text description of the collection."
        )
    ] = None
    icon_url: Annotated[
        HttpUrl | None,
        Field(
            example="https://live.staticflickr.com/3091/2883561418_dafc36c92b_z.jpg",
            description="A url to an image icon for the collection."
        )
    ] = None
    attribution: Annotated[
        str | None,
        Field(
            min_length=1,
            max_length=10000,
            example="This collection was contributed by the <organization here> "
                + "project. For more details, see <DOI here> and <website here>. "
                + "\n\nFunding was provided by <funding organization here> grant number <#>.",
            description="Markdown text describing the source of the data and to whom credit "
                + "belongs. DOI and citation information may be included."
        )
    ] = None
    data_products: list[DataProduct] = Field(
        description="The data products associated with the collection"
    )
    matchers: list[Matcher] = Field(
        description="The matchers associated with the collection"
    )
    default_select: Annotated[
        str | None,
        Field(  # might need to make this a list in future...? not sure
            **DATA_PRODUCT_ID_FIELD_PROPS | {
            "description":
                "The ID of the data product to which non-data product specific selections "
                + "should be applied. If present, the data product must be listed in the data "
                + "products list. If absent, most selections will fail."
            }
        )
    ] = None

    @field_validator("name", "ver_src", mode="before")
    @classmethod
    def _strip_and_fail_on_control_characters(cls, v):
        v = v.strip()
        pos = contains_control_characters(v)
        if pos > -1:
            raise ValueError(f"contains a control character at position {pos}")
        return v

    @field_validator("desc", "attribution", mode="before")
    @classmethod
    def _strip_and_fail_on_control_characters_with_exceptions(cls, v):
        if v is None:
            return None
        v = v.strip()
        pos = contains_control_characters(v, allow_tab_newline=True)
        if pos > -1: 
            raise ValueError(f"contains a non tab or newline control character at position {pos}")
        return v

    @field_validator("data_products")
    @classmethod
    def _check_data_products_unique(cls, v):
        return cls._checkunique(v, lambda dp: dp.product, "data product")

    @field_validator("matchers")
    @classmethod
    def _check_matchers_unique(cls, v):
        return cls._checkunique(v, lambda m: m.matcher, "matcher")

    @classmethod
    def _checkunique(cls, items, accessor, field):
        seen = set()
        for it in items:
            if accessor(it) in seen:
                raise ValueError(f"duplicate {field}: {accessor(it)}")
            seen.add(accessor(it))
        return items

    @model_validator(mode="after")
    def _check_default_selection(self) -> Self:
        if self.default_select:
            dps = {dp.product for dp in self.data_products}
            if self.default_select not in dps:
                raise ValueError(f"The default selection data product {self.default_select} "
                    + "is not in the set of specified data products"
                )
        return self
    
    def get_data_product(self, data_product) -> DataProduct:
        """ Get a data product by its ID. """
        for dp in self.data_products:
            if dp.product == data_product:
                return dp
        raise ValueError(f"No such data product: {data_product}")


# No need to worry about field validation here since the service is assigning the values
# Re the dates, since Arango doesn't have a special format for dates like mongo, we might as
# well store them as human readable strings. ISO8601 means they'll sort in correct order if
# necessary.
class SavedCollection(Collection):
    """
    A collection version returned from the collections service.
    """
    id: str = Field(
        example=FIELD_COLLECTION_ID_EXAMPLE,
        description=FIELD_COLLECTION_ID_DESCRIPTION
    )
    ver_tag: str = Field(
        example=FIELD_VER_TAG_EXAMPLE,
        description=FIELD_VER_TAG_DESCRIPTION
    )
    ver_num: int = Field(
        example=FIELD_VER_NUM_EXAMPLE,
        description=FIELD_VER_NUM_DESCRIPTION
    )
    date_create: str = Field(
        example="2022-10-07T17:58:53.188698+00:00",
        description="The date the collection version was created in ISO8061 format"
    )
    user_create: str = Field(
        example="kbasehelp",
        description="The user that created the collection version."
    )


class ActiveCollection(SavedCollection):
    """
    An active collection version document returned from the collections service.
    """
    date_active: str = Field(
        example="2022-10-07T17:59:53.188698+00:00",
        description="The date the collection version was set as the active version in "
            + "ISO8601 format."
    )
    user_active: str = Field(
        example="otherkbasehelp",
        description="The user that activated the collection version."
    )


# https://fastapi.tiangolo.com//tutorial/path-params/#create-an-enum-class
class ProcessState(str, Enum):
    """
    The state of a process.
    """
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"


class ProcessStateField(BaseModel):  # for lack of a better name
    state: ProcessState = Field(
        example=ProcessState.PROCESSING.value,
        description="The state of the process associated with this data."
    )

    def is_complete(self):
        return self.state == ProcessState.COMPLETE


class ProcessAttributes(ProcessStateField):
    created: int = Field(
        example=1674243789864,
        description="Milliseconds since the Unix epoch at the point the data and "
            + "corresponding process was created."
    )
    heartbeat: Annotated[
        int | None,
        Field(
            example=1674243789866,
            description="Milliseconds since the Unix epoch at the last time the process sent "
                + "a heartbeat. Used to determine when the process needs to be restarted."
        )
    ] = None
    # Note this means that processes should be idempotent - running the same process twice,
    # even if one of the processes was interrupted, should produce the same result when at
    # least one process completes
    # TODO DOCS document the above.

    state_updated: int = Field(
        example=1674243789867,
        description="Milliseconds since the Unix epoch at the point the process state was last "
            + "updated."
    )


class LastAccess(BaseModel):
    last_access: int = Field(
        example=1674243789865,
        description="Milliseconds since the Unix epoch at the point this data was last accessed. "
            + "Used for determining when to delete the data."
    )


class CollectionSpec(BaseModel):
    """ Specifies the name and numerical version of a collection. """
    collection_id: str = Field(
        example=FIELD_COLLECTION_ID_EXAMPLE,
        description="The ID of the collection."
    )
    collection_ver: int = Field(
        example=7,
        description="The version of the collection."
    )


class Match(CollectionSpec, ProcessStateField):
    """
    A match between KBase workspace service objects and data in a collection.
    """
    match_id: str = Field(
        description="The ID of the match; a unique but opaque string."
        # In practice, this ID is the MD5 of
        # * the matcher ID
        # * the collection ID and version
        # * the match user parameters
        # * the input workspace service UPAs, sorted
    )
    matcher_id: str = Field(
        example="gtdb_lineage",
        description="The ID of the matcher performing the match."
    )
    user_parameters: dict[str, Any] = Field(
        example=FIELD_USER_PARAMETERS_EXAMPLE,
        description=FIELD_USER_PARAMETERS_DESCRIPTION,
    )
    # Strictly speaking this could be looked up from the db with the collection id, ver, and
    # matcher ID, but it's immutable given those parameters and so saves a DB lookup to store it
    # here as well
    collection_parameters: dict[str, Any] = Field(
        example=FIELD_MATCHER_PARAMETERS_EXAMPLE,
        description=FIELD_MATCHER_PARAMETERS_DESCRIPTION,
    )
    upa_count: Annotated[int, Field(
        example=10,
        description="The number of UPAs involved in the match. The count is taken after "
            + "expanding any sets in the input."
    )]
    match_count: Annotated[int | None, Field(
        example=121,
        description="The number of matches in the match if the match has completed, or null "
            + "otherwise."
    )] = None


class MatchVerbose(Match):
    """
    A match with the details of the match, e.g. input UPAs and resulting matching collection IDs.
    """
    # could add a UPA regex check here but that seems expensive and redundant. Will need to
    # parse the UPAs elsewhere anyway
    upas: list[str] = Field(
        example=FIELD_UPA_LIST_EXAMPLE,
        description=FIELD_UPA_LIST_DESCRIPTION,
    )
    matches: list[str] | None = Field(
        example=["GCA_000188315.1", "GCA_000172955.1"],
        description="Unique identifiers for the matches in the collection. The contents of the "
            + "strings will differ based on the matcher and the collection in question. "
            + "Null if the match is not yet complete."
    )   # This might get hairy. Hopefully we can lock this down a bit with more experience in
        # how this is going to work.
        # E.g. should it be the Arango _key value instead of the genome_id value in the
        # case of a gtdb_lineage matcher? The former is not useful to users, but is guaranteed
        # to be unique. However, the loaders should be guaranteeing uniqueness of the
        # genome_ids.
        # Do we need to be able to map the incoming UPAs to their matches? Potentially 1:M
        # Maybe this field should be internal only?


class InternalMatch(MatchVerbose, ProcessAttributes, LastAccess):
    """
    Holds match fields for internal server use.
    """
    # We keep the created time internal since matches are not user specific. One user could
    # "create" a match but have it be really old. Avoid the confusion, keep it internal
    internal_match_id: str = Field(
        # bit of a long field name but probably not too many matches at one time
        example="e22f2d7d-7246-4636-a91b-13f29bc32d3d",
        description="An internal ID for the match that is unique per use. This allows for "
            + "deleting data for a match without the risk that a new match with the same "
            + "md5 ID is created and tries to read data in the process of deletion. "
            + "Expected to be a v4 UUID.",
    )
    wsids: set[int] = Field(
        example={78, 10067},
        description="The set of workspace IDs from the UPAs."
    )
    user_last_perm_check: dict[str, int] = Field(
        example={"user1": 1674243789451},
        description="A mapping of user name to the last time their permissions to view the "
            + "match was checked in Unix epoch milliseconds. Used to determine when to recheck "
            + "permissions for a user."
    )


class DeletedMatch(InternalMatch):
    """ A match in the deleted state, waiting for permanent deletion. """
    deleted: int = Field(
        example=1674243789870,
        description="Milliseconds since the Unix epoch at the point the match was deleted."
    )


class SubsetType(str, Enum):
    """ The type of a data subset. """

    MATCH = "match"
    """ A subset based on a data match. """

    SELECTION = "selection"
    """ A subset based on a user selection. """


class DataProductProcessIdentifier(BaseModel):
    """
    Uniquely identifies a data product process based on the internal ID of the parent data,
    the data product in question, and the type of the parent data (and therefore the type
    of the process).
    """
    internal_id: str = Field(
        example="e22f2d7d-7246-4636-a91b-13f29bc32d3d",
        description="An internal ID for the match or selection that is unique per use. "
            + "This allows for deleting data without the risk that new data with the same "
            + "ID is created and tries to read data in the process of deletion. "
            + "Expected to be a v4 UUID.",
    )
    data_product: str = DATA_PRODUCT_ID_FIELD
    type: SubsetType = Field(
        example=SubsetType.SELECTION.value,
        description="The type of data the process is acting on."
    )

    def is_match(self):
        return self.type == SubsetType.MATCH


class DataProductProcess(DataProductProcessIdentifier, ProcessAttributes):
    """
    Defines the state of processing for a data product that was not part of the primary
    match or selection process.

    For example, a gtdb_lineage match will match against the genome_attributes data product
    as part of the primary match since the data being matched against is in that data product.
    However, calculating the match for the taxa_count data product requires that the primary
    match is complete first. This class represents the state of calculating the match for a
    non-primary data product like taxa_count.
    """
    # last access / user perms are tracked in the primary match document. When that document
    # is deleted in the DB, this one should be as well (after deleting any data associated with
    # the match).
    missing_ids: Annotated[list[str] | None, Field(
        example=FIELD_SELECTION_EXAMPLE,
        description="Any IDs that were not found during the match or selection processing but "
            + "were not in the original match. This may happen normally if a data product "
            + "depends on data that is not available at the data source for a subset of the "
            + "data units at the data source."
    )] = None


class Selection(CollectionSpec, ProcessStateField):
    """
    A user selected set of data in a collection.
    """

    selection_id: str = Field(
        description="The ID of the selection; a unique but opaque string."
        # In practice, this ID is the MD5 of
        # * the collection ID and version
        # * the selection data, sorted
    )
    selection_count: Annotated[int, Field(
        example=10,
        description="The number of items in the selection."
    )]
    unmatched_count: Annotated[int | None, Field(
        example=121,
        description="The number of selection IDs that didn't match any data if the "
            + "selection completed, or null otherwise."
    )] = None


class SelectionVerbose(Selection):
    """
    A selection including the selection data, e.g. the data IDs and which, if any, were unable
    to be matched to the data in the selection.
    """

    selection_ids: list[str] = Field(
        example=FIELD_SELECTION_EXAMPLE,
        description=FIELD_SELECTION_IDS_DESCRIPTION
    )
    unmatched_ids: Annotated[
        list[str] | None,
        Field(
            example=FIELD_SELECTION_EXAMPLE,
            description=FIELD_SELECTION_UNMATCHED_DESCRIPTION
        )
    ] = None


class InternalSelection(SelectionVerbose, ProcessAttributes, LastAccess):
    """
    Internal details for the selection.
    """
    # We keep the created time internal since selections are not user specific. One user could
    # "create" a selection but have it be really old. Avoid the confusion, keep it internal

    internal_selection_id: str = Field(
        example="e22f2d7d-7246-4636-a91b-13f29bc32d3d",
        description="An internal ID for the selection that is unique per use. This allows for "
            + "deleting data for a selection without the risk that the selection will become "
            + "active again while the data is being deleted. "
            + "Expected to be a v4 UUID.",
    )
    data_product: str = Field(
        **DATA_PRODUCT_ID_FIELD_PROPS | {
        "description":
            "The ID of the data product to which the selection should be applied."
        }
    )


class DeletedSelection(InternalSelection):
    """ A selection in the deleted state, waiting for permanent deletion. """
    deleted: int = Field(
        example=1674243789870,
        description="Milliseconds since the Unix epoch at the point the selection was deleted."
    )


def remove_non_model_fields(doc: dict, model: BaseModel) -> dict:
    """
    Removes any fields in `doc` that aren't fields in the pydantic model.
    """
    modelfields = set(model.__fields__.keys())
    return {f: doc[f] for f in doc if f in modelfields}
