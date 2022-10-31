"""
Pydantic models for service error structures.
"""

from pydantic import BaseModel, Field
from typing import Optional, Union
from src.service.errors import ErrorType


class ServerErrorDetail(BaseModel):
    httpcode: int = Field(example=500, description="The HTTP error code")
    httpstatus: str = Field(example="INTERNAL SERVER ERRROR", description="The HTTP status string")
    time: str = Field(
        example="2022-10-07T17:58:53.188698+00:00",
        description="The server time in ISO8601 format"
    )
    message: Optional[str] = Field(
        example="Well dang, that ain't good",
        description="A free text string providing more information about the error"
    )

class RequestValidationDetail(BaseModel):
    # Structure from https://github.com/tiangolo/fastapi/blob/f67b19f0f73ebdca01775b8c7e531e51b9cecfae/fastapi/openapi/utils.py#L34-L59
    # Note I have witnessed other fields in the response as well, which apparently aren't
    # included in the spec
    loc: list[Union[str, int]] = Field(
        example=["body", "data_products", 2, "version"],
        description="The location where the validation error occured"
    )
    msg: str = Field(
        example="ensure this value has at most 20 characters",
        description="A free text message explaining the validation problem"
    )
    type: str = Field(
        example="value_error.any_str.max_length",
        description="The type of the validation error"
    )


class ClientErrorDetail(ServerErrorDetail):
    httpcode: int = Field(example=400, description="The HTTP error code")
    httpstatus: str = Field(example="BAD REQUEST", description="The HTTP status string")
    appcode: Optional[int] = Field(
        example=30010,
        description="An application code providing more specific information about an error, "
            + "if available"
    )
    apperror: Optional[str] = Field(
        example="Request validation failed",
        description="The error string for the application error code. If the error code is "
            + "available, the string is always available"
    )
    request_validation_detail: Optional[list[RequestValidationDetail]] = Field(
        description=
            "Information about why a request failed to pass the FastAPI validation system. "
            + f'Included when the app error is "{ErrorType.REQUEST_VALIDATION_FAILED.error_type}".' 
    )


class ServerError(BaseModel):
    """ An server error uncaused by the client. """
    error: ServerErrorDetail


class ClientError(BaseModel):
    """ An error caused by a bad client request. """
    error: ClientErrorDetail
