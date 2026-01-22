from typing import Any

from pyhiera.errors import PyHieraError
from pyhiera.errors import PyHieraBackendError
from pyhiera.backends import PyHieraBackend, PyHieraBackendData
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
                return self.key_data_validate(key, data[0].data)
        raise PyHieraBackendError("No data found")

    def key_data_get_merge(
        self,
        key: str,
        facts: dict[str, str],
    ) -> Any:
        if key not in self.keys:
            raise PyHieraError(f"Key {key} not found")

        data_points = []
        for backend in self.backends:
            _data_points = backend.key_data_get(key, facts)
            if _data_points:
                for data_point in _data_points:
                    if not isinstance(data_point.data, dict):
                        raise PyHieraBackendError(
                            f"Invalid data for key {key}, expected dict, got: {data_point.data}"
                        )
                    data_point.data = self.key_data_validate(
                        key, data_point.data
                    ).model_dump(exclude_none=True)["data"]
                    data_points.append(data_point)

        if not data_points:
            raise PyHieraBackendError("No data found")

        merged_data = {}
        for data_point in reversed(data_points):
            print(f"Merging data from {data_point.level}: {data_point.data}")
            merged_data = self._key_data_get_merge(data_point.data, merged_data)

        return self.key_data_validate(key, merged_data)

    def _key_data_get_merge(self, update, result):
        for key, value in update.items():
            if isinstance(value, dict):
                self._key_data_get_merge(value, result.setdefault(key, {}))
            elif isinstance(value, list):
                if key in result:
                    result[key].extend(value)
                else:
                    result[key] = value
            elif isinstance(value, set):
                if key in result:
                    result[key].update(value)
                else:
                    result[key] = value
            else:
                result[key] = value
        return result
