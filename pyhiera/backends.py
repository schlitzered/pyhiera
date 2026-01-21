import os
from typing import Any

from pydantic import BaseModel

import yaml

from pyhiera.errors import PyHieraBackendError


class PyHieraBackendData(BaseModel):
    level: str
    data: Any


class PyHieraBackend:
    def __init__(
        self,
        config: dict[str, str],
        hierarchy: list[str] = None,
    ):
        self._config = config
        self._hierarchy = hierarchy
        self.init()

    @property
    def config(self):
        return self._config

    @property
    def hierarchy(self) -> list[str]:
        return self._hierarchy

    def init(self):
        pass

    @staticmethod
    def _expand_level(level: str, facts: dict[str, str]) -> str:
        try:
            return level.format(**facts)
        except KeyError as err:
            raise PyHieraBackendError(f"missing facts to expand level {level}: {err}")

    def key_data_add(
        self,
        key: str,
        data: Any,
        level: str,
        facts: dict[str, str],
    ):
        if level not in self.hierarchy:
            raise PyHieraBackendError(f"Level {level} not found in hierarchy")
        return self._key_data_add(
            key,
            data,
            self._expand_level(level, facts),
        )

    def _key_data_add(
        self,
        key: str,
        data: Any,
        level: str,
    ):
        raise NotImplementedError

    def key_data_get(
        self,
        key: str,
        facts: dict[str, str],
    ) -> Any:
        levels = list()
        for level in self.hierarchy:
            levels.append(self._expand_level(level, facts))
        return self._key_data_get(key, levels)

    def _key_data_get(
        self,
        key: str,
        levels: list[str],
    ) -> list[PyHieraBackendData]:
        raise NotImplementedError


class PyHieraBackendYaml(PyHieraBackend):

    def __init__(
        self,
        config: dict[str, str],
        hierarchy: list[str] = None,
    ):
        self._base_path = None
        super().__init__(
            config=config,
            hierarchy=hierarchy,
        )

    def init(self):
        self._base_path = self.config["path"]

    @property
    def base_path(self):
        return self._base_path

    def _key_data_add(
        self,
        key: str,
        data: Any,
        level: str,
    ):
        try:
            file_name = f"{self.base_path}/{level}"
            if not os.path.exists(os.path.dirname(file_name)):
                os.makedirs(os.path.dirname(file_name))
            with open(f"{self.base_path}/{level}", "r") as f:
                content = yaml.safe_load(f) or {}
        except FileNotFoundError:
            content = dict()
        content[key] = data
        with open(f"{self.base_path}/{level}", "w") as f:
            yaml.dump(content, f)

    def _key_data_get(
        self,
        key: str,
        levels: dict[str, Any],
    ) -> list[PyHieraBackendData]:
        result = list()
        for level in levels:
            try:
                with open(f"{self.base_path}/{level}", "r") as f:
                    data = yaml.safe_load(f)
                    result.append(PyHieraBackendData(level=level, data=data[key]))
            except FileNotFoundError:
                pass
            except KeyError:
                pass
        return result
