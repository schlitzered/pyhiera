from typing import Any

from pyhiera.models import PyHieraDataBase
from pyhiera.models import PyHieraDataString
from pyhiera.models import PyHieraDataInt
from pyhiera.models import PyHieraDataFloat
from pyhiera.models import PyHieraDataBool
from pyhiera.models import PyHieraKeyDataComplex


class PyHieraKeyBase:
    def __init__(self):
        self._description = "something useful"
        self._model = PyHieraDataBase

    @property
    def description(self) -> str:
        return self._description

    @property
    def model(self) -> type[PyHieraDataBase]:
        return self._model

    def validate(self, data: Any) -> PyHieraDataBase:
        return self._model(data=data)


class PyHieraKeyString(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple string"
        self._model = PyHieraDataString


class PyHieraKeyInt(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple int"
        self._model = PyHieraDataInt


class PyHieraKeyFloat(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple float"
        self._model = PyHieraDataFloat


class PyHieraKeyBool(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "simple bool"
        self._model = PyHieraDataBool


class PyHieraKeyComplex(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "complex data"
        self._model = PyHieraKeyDataComplex
