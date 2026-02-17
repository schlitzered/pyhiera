"""Pydantic models for PyHiera data validation and structure."""

from typing import Any, Optional
from pydantic import BaseModel


class PyHieraModelBackendData(BaseModel):
    """Model representing data retrieved from a backend.

    Tracks the source and metadata of hierarchical data for debugging
    and source attribution.

    Attributes:
        identifier: Backend identifier that provided this data.
        priority: Priority of the backend (for ordering).
        level: Hierarchy level where data was found.
        key: Key name for the data.
        data: The actual data value (any type).
    """

    identifier: str
    priority: int
    level: str
    key: str
    data: Any


class PyHieraModelDataBase(BaseModel):
    """Base model for validated hierarchical data.

    Wraps data with optional source tracking for transparency about
    where data originated from in the hierarchy.

    Attributes:
        sources: Optional list of backend sources that provided this data.
        data: The actual data value (any type).
    """

    sources: Optional[list[PyHieraModelBackendData]] = None
    data: Any


class PyHieraModelDataBool(PyHieraModelDataBase):
    """Model for boolean data values.

    Attributes:
        data: Boolean value.
    """

    data: bool


class PyHieraModelDataString(PyHieraModelDataBase):
    """Model for string data values.

    Attributes:
        data: String value.
    """

    data: str


class PyHieraModelDataInt(PyHieraModelDataBase):
    """Model for integer data values.

    Attributes:
        data: Integer value.
    """

    data: int


class PyHieraModelDataFloat(PyHieraModelDataBase):
    """Model for floating-point data values.

    Attributes:
        data: Float value.
    """

    data: float
