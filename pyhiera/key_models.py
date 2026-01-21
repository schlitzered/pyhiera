from typing import Any
from pydantic import BaseModel


class PyHieraKeyModelBase(BaseModel):
    data: Any


class PyHieraKeyModelString(PyHieraKeyModelBase):
    data: str


class PyHieraKeyModelInt(PyHieraKeyModelBase):
    data: int


class PyHieraKeyModelFloat(PyHieraKeyModelBase):
    data: float


class PyHieraKeyModelBool(PyHieraKeyModelBase):
    data: bool


class PyHieraKeyBase:
    def __init__(self):
        self._description = "something useful"
        self._model = PyHieraKeyModelBase

    @property
    def description(self) -> str:
        return self._description

    @property
    def model(self) -> type[PyHieraKeyModelBase]:
        return self._model

    def validate(self, data: Any) -> PyHieraKeyModelBase:
        return self._model(data=data)


class PyHieraKeyString(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple string"
        self._model = PyHieraKeyModelString


class PyHieraKeyInt(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple int"
        self._model = PyHieraKeyModelInt


class PyHieraKeyFloat(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple float"
        self._model = PyHieraKeyModelFloat


class PyHieraKeyBool(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple bool"
        self._model = PyHieraKeyModelBool


class PyHieraKeyModelComplexLevelB(BaseModel):
    blarg: str


class PyHieraKeyModelComplexLevel(BaseModel):
    a: str
    b: PyHieraKeyModelComplexLevelB


class PyHieraKeyModelComplex(PyHieraKeyModelBase):
    data: PyHieraKeyModelComplexLevel


class PyHieraKeyComplex(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "complex data"
        self._model = PyHieraKeyModelComplex
