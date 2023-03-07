"""
A test matcher for use while the service only has one "real" matcher.
"""

from pydantic import BaseModel, Field

from src.service.matchers.common_models import Matcher

# TODO PRODUCTION delete this matcher

class TestMatcherCollectionParameters(BaseModel):
    "Parameters for the test matcher."
    foobar: str = Field(
        example="207.0",
        description="The GTDB version of the collection in which the matcher is installed. " +
            "Input data to the matcher must match this version of GTDB or the match will " +
            "abort.",
        regex=r"^\d{2,4}\.\d{1,2}$"  # giving a little room for expansion
    )


MATCHER = Matcher(
    id="test",
    description="stuff",
    types=["KBaseGenomes.Genome", "KBaseGenomeAnnotations.Assembly"],
    set_types=[],
    required_data_products=[],
    user_parameters=None,
    collection_parameters=TestMatcherCollectionParameters.schema()
)
