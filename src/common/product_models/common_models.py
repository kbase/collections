"""
Common models shared between data products as well as loaders.
"""

from pydantic import BaseModel, Field
from src.service import models  # maybe we should move all the models to common? Yuck


# these fields need to match the fields in the models below.
FIELD_MATCH_STATE = "match_state"
FIELD_SELECTION_STATE = "selection_state"


class SubsetProcessStates(BaseModel):
    """
    The state of the match and / or selection subsetting process for a particular data product.
    """
    match_state: models.ProcessState | None = Field(
        example=models.ProcessState.PROCESSING,
        description="The processing state of the match (if any) for this data product. "
            + "This data product requires additional processing beyond the primary match."
    )
    selection_state: models.ProcessState | None = Field(
        example=models.ProcessState.FAILED,
        description="The processing state of the selection (if any) for this data product. "
            + "This data product requires additional processing beyond the primary selection."
    )
