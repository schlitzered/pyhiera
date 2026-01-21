from typing import Any

from pyhiera.errors import PyHieraError
from pyhiera.errors import PyHieraBackendError
from pyhiera.backends import PyHieraBackend
from pyhiera.backends import PyHieraBackendYaml
from pyhiera.key_models import PyHieraKeyBase
from pyhiera.key_models import PyHieraKeyString
from pyhiera.key_models import PyHieraKeyInt
from pyhiera.key_models import PyHieraKeyFloat
from pyhiera.key_models import PyHieraKeyBool
from pyhiera.key_models import PyHieraKeyComplex
from pyhiera.key_models import PyHieraKeyModelBase


class PyHiera:
    def __init__(
        self,
        backends: list[PyHieraBackend],
    ):
        self._backends = backends
        self._keys = dict()
        self._key_models = {
            "SimpleString": PyHieraKeyString,
            "SimpleInt": PyHieraKeyInt,
            "SimpleFloat": PyHieraKeyFloat,
            "SimpleBool": PyHieraKeyBool,
            "Complex": PyHieraKeyComplex,
        }

    @property
    def backends(self) -> list[PyHieraBackend]:
        return self._backends

    @property
    def keys(self) -> dict[str, PyHieraKeyBase]:
        return self._keys

    @property
    def key_models(self) -> dict[str, type[PyHieraKeyBase]]:
        return self._key_models

    def key_add(self, key: str, hiera_key: str):
        if hiera_key not in self.key_models:
            raise PyHieraError(f"Invalid key model {hiera_key}")
        self._keys[key] = self.key_models[hiera_key]()

    def key_delete(self, key: str):
        del self._keys[key]

    def key_data_validate(self, key: str, data: Any) -> PyHieraKeyModelBase:
        try:
            return self.keys[key].model(data=data)
        except KeyError:
            raise PyHieraError(f"Key {key} not found")
        except ValueError as err:
            raise PyHieraError(f"Invalid data for key {key}: {err}")

    def key_data_get(
        self,
        key: str,
        facts: dict[str, str],
    ) -> Any:
        if key not in self.keys:
            raise PyHieraError(f"Key {key} not found")
        for backend in self.backends:
            data = backend.key_data_get(key, facts)
            if data:
                model = self.key_data_validate(key, data[0].data)
                return model
        raise PyHieraBackendError("No data found")
