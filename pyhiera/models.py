from typing import Any, Optional
from pydantic import BaseModel


class PyHieraBackendData(BaseModel):
    identifier: str
    priority: int
    level: str
    key: str
    data: Any


class PyHieraDataBase(BaseModel):
    sources: Optional[list[PyHieraBackendData]] = None
    data: Any


class PyHieraDataString(PyHieraDataBase):
    data: str


class PyHieraDataInt(PyHieraDataBase):
    data: int


class PyHieraDataFloat(PyHieraDataBase):
    data: float


class PyHieraDataBool(PyHieraDataBase):
    data: bool


class PyHieraKeyDataComplexLevelB(BaseModel):
    blarg: Optional[str] = None
    other: Optional[str] = None
    blub: Optional[set[str]] = None


class PyHieraKeyDataComplexLevel(BaseModel):
    a: Optional[str] = None
    b: Optional[PyHieraKeyDataComplexLevelB] = None


class PyHieraKeyDataComplex(PyHieraDataBase):
    data: PyHieraKeyDataComplexLevel
