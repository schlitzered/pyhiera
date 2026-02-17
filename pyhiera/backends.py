import logging
import os
from typing import Any

import aiofiles
import aiofiles.os
import yaml

from pyhiera.errors import PyHieraBackendError
from pyhiera.models import PyHieraModelBackendData
from pyhiera.models import PyHieraModelDataBase

logger = logging.getLogger(__name__)


class PyHieraBackendBase:
    def __init__(
        self,
        config: dict[str, str],
        identifier: str,
        priority: int,
        hierarchy: list[str],
    ):
        self._config = config
        self._hierarchy = hierarchy
        self._identifier = identifier
        self._priority = priority
        self.init()

    @property
    def config(self):
        return self._config

    @property
    def hierarchy(self) -> list[str]:
        return self._hierarchy

    @property
    def identifier(self):
        return self._identifier

    @property
    def priority(self):
        return self._priority

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
        data: PyHieraModelDataBase,
        level: str,
        facts: dict[str, str],
    ):
        raise NotImplementedError

    def key_data_get(
        self,
        key: str,
        facts: dict[str, str],
    ) -> Any:
        raise NotImplementedError


class PyHieraBackendSync(PyHieraBackendBase):
    def key_data_add(
        self,
        key: str,
        data: PyHieraModelDataBase,
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
        data: PyHieraModelDataBase,
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
    ) -> list[PyHieraModelBackendData]:
        raise NotImplementedError


class PyHieraBackendAsync(PyHieraBackendBase):
    async def key_data_add(
        self,
        key: str,
        data: PyHieraModelDataBase,
        level: str,
        facts: dict[str, str],
    ):
        if level not in self.hierarchy:
            raise PyHieraBackendError(f"Level {level} not found in hierarchy")
        return await self._key_data_add(
            key,
            data,
            self._expand_level(level, facts),
        )

    async def _key_data_add(
        self,
        key: str,
        data: PyHieraModelDataBase,
        level: str,
    ):
        raise NotImplementedError

    async def key_data_get(
        self,
        key: str,
        facts: dict[str, str],
    ) -> Any:
        levels = list()
        for level in self.hierarchy:
            levels.append(self._expand_level(level, facts))
        return await self._key_data_get(key, levels)

    async def _key_data_get(
        self,
        key: str,
        levels: list[str],
    ) -> list[PyHieraModelBackendData]:
        raise NotImplementedError


class PyHieraBackendYamlAsync(PyHieraBackendAsync):
    def __init__(
        self,
        config: dict[str, str],
        identifier: str,
        priority: int,
        hierarchy: list[str],
    ):
        self._base_path = None
        super().__init__(
            config=config,
            identifier=identifier,
            priority=priority,
            hierarchy=hierarchy,
        )

    def init(self):
        self._base_path = self.config["path"]

    @property
    def base_path(self):
        return self._base_path

    async def _key_data_add(
        self,
        key: str,
        data: PyHieraModelDataBase,
        level: str,
    ):
        file_name = os.path.join(self.base_path, level)
        try:
            dir_name = os.path.dirname(file_name)
            if not await aiofiles.os.path.exists(dir_name):
                await aiofiles.os.makedirs(dir_name, exist_ok=True)
            async with aiofiles.open(file_name, "r") as f:
                content = yaml.safe_load(await f.read()) or {}
        except FileNotFoundError:
            content = dict()
        if not isinstance(content, dict):
            content = {}
        # Serialize data: if it's a Pydantic model, dump it; otherwise use directly
        if hasattr(data.data, 'model_dump'):
            content[key] = data.data.model_dump()
        else:
            content[key] = data.data
        async with aiofiles.open(file_name, "w") as f:
            await f.write(yaml.dump(content))
        logger.debug(f"Added data for key '{key}' to {file_name}")

    async def _key_data_get(
        self,
        key: str,
        levels: list[str],
    ) -> list[PyHieraModelBackendData]:
        result = list()
        for level in levels:
            try:
                file_name = os.path.join(self.base_path, level)
                async with aiofiles.open(file_name, "r") as f:
                    data = yaml.safe_load(await f.read())
                if not isinstance(data, dict):
                    continue
                if key not in data:
                    continue
                result.append(
                    PyHieraModelBackendData(
                        identifier=self.identifier,
                        priority=self.priority,
                        key=key,
                        level=level,
                        data=data[key],
                    ),
                )
                logger.debug(f"Found key '{key}' in {file_name}")
            except OSError as e:
                logger.debug(f"Failed to read {file_name}: {e}")
            except yaml.YAMLError as e:
                logger.warning(f"Invalid YAML in {file_name}: {e}")
        return result


class PyHieraBackendYamlSync(PyHieraBackendSync):
    def __init__(
        self,
        config: dict[str, str],
        identifier: str,
        priority: int,
        hierarchy: list[str],
    ):
        self._base_path = None
        super().__init__(
            config=config,
            identifier=identifier,
            priority=priority,
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
        data: PyHieraModelDataBase,
        level: str,
    ):
        file_name = os.path.join(self.base_path, level)
        try:
            dir_name = os.path.dirname(file_name)
            if not os.path.exists(dir_name):
                os.makedirs(dir_name, exist_ok=True)
            with open(file_name, "r") as f:
                content = yaml.safe_load(f) or {}
        except FileNotFoundError:
            content = dict()
        if not isinstance(content, dict):
            content = {}
        # Serialize data: if it's a Pydantic model, dump it; otherwise use directly
        if hasattr(data.data, 'model_dump'):
            content[key] = data.data.model_dump()
        else:
            content[key] = data.data
        with open(file_name, "w") as f:
            yaml.dump(content, f)
        logger.debug(f"Added data for key '{key}' to {file_name}")

    def _key_data_get(
        self,
        key: str,
        levels: list[str],
    ) -> list[PyHieraModelBackendData]:
        result = list()
        for level in levels:
            try:
                file_name = os.path.join(self.base_path, level)
                with open(file_name, "r") as f:
                    data = yaml.safe_load(f)
                    if not isinstance(data, dict):
                        continue
                    if key not in data:
                        continue
                    result.append(
                        PyHieraModelBackendData(
                            identifier=self.identifier,
                            priority=self.priority,
                            key=key,
                            level=level,
                            data=data[key],
                        ),
                    )
                    logger.debug(f"Found key '{key}' in {file_name}")
            except OSError as e:
                logger.debug(f"Failed to read {file_name}: {e}")
            except yaml.YAMLError as e:
                logger.warning(f"Invalid YAML in {file_name}: {e}")
        return result
