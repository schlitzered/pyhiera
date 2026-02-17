import logging
from typing import Any
from typing import Optional

from pydantic import ValidationError

from pyhiera.errors import PyHieraError
from pyhiera.errors import PyHieraBackendError
from pyhiera.backends import PyHieraBackendBase
from pyhiera.backends import PyHieraBackendAsync
from pyhiera.backends import PyHieraBackendSync
from pyhiera.keys import PyHieraKeyBase
from pyhiera.keys import PyHieraKeyString
from pyhiera.keys import PyHieraKeyInt
from pyhiera.keys import PyHieraKeyFloat
from pyhiera.keys import PyHieraKeyBool
from pyhiera.keys import PyHieraModelDataBase
from pyhiera.models import PyHieraModelBackendData

logger = logging.getLogger(__name__)

# Default key models available out of the box
DEFAULT_KEY_MODELS = {
    "SimpleString": PyHieraKeyString,
    "SimpleInt": PyHieraKeyInt,
    "SimpleFloat": PyHieraKeyFloat,
    "SimpleBool": PyHieraKeyBool,
}


class PyHieraKeyModels:
    """Registry for managing key model types.

    Key models define the validation schema and data types for hierarchical keys.
    This class provides a registry to add, remove, and retrieve model types.

    Attributes:
        models: Dictionary mapping model names to PyHieraKeyBase subclasses.
    """

    def __init__(
        self,
        models: Optional[dict[str, type[PyHieraKeyBase]]] = None,
    ):
        """Initialize key models registry.

        Args:
            models: Optional dictionary of model name to PyHieraKeyBase subclass.
                   If None, initializes with DEFAULT_KEY_MODELS.
        """
        if models is None:
            models = DEFAULT_KEY_MODELS.copy()
        self._models = models
        logger.debug(f"Initialized key models registry with {len(models)} models")

    @property
    def models(self) -> dict[str, type[PyHieraKeyBase]]:
        """Get dictionary of registered models.

        Returns:
            Dictionary mapping model names to PyHieraKeyBase subclasses.

        Note:
            Returns a copy to prevent external mutation of internal state.
        """
        return self._models.copy()

    def add(self, key: str, model: type[PyHieraKeyBase]):
        """Register a new key model.

        Args:
            key: Unique identifier for the model.
            model: PyHieraKeyBase subclass to register.

        Raises:
            PyHieraError: If model is not a valid PyHieraKeyBase subclass.
        """
        try:
            if not issubclass(model, PyHieraKeyBase):
                raise PyHieraError(
                    f"Model must be a subclass of PyHieraKeyBase, got {model}"
                )
        except TypeError:
            raise PyHieraError(
                f"Model must be a class (subclass of PyHieraKeyBase), got {model}"
            )
        self._models[key] = model
        logger.info(f"Added key model: {key} -> {model.__name__}")

    def delete(self, key: str):
        """Remove a key model from the registry.

        Args:
            key: Model identifier to remove.

        Raises:
            PyHieraError: If model key not found in registry.
        """
        try:
            del self._models[key]
            logger.info(f"Deleted key model: {key}")
        except KeyError:
            raise PyHieraError(f"Key model {key} not found")

    def get(self, key: str) -> type[PyHieraKeyBase]:
        """Retrieve a key model from the registry.

        Args:
            key: Model identifier to retrieve.

        Returns:
            PyHieraKeyBase subclass.

        Raises:
            PyHieraError: If model key not found in registry.
        """
        try:
            return self._models[key]
        except KeyError:
            raise PyHieraError(f"Invalid key model {key}")


class PyHieraKeys:
    """Registry for managing hierarchical keys.

    Keys are instances of key models that define the structure and validation
    for data retrieved from backends.

    Attributes:
        keys: Dictionary mapping key names to PyHieraKeyBase instances.
    """

    def __init__(self, key_models: PyHieraKeyModels):
        """Initialize keys registry.

        Args:
            key_models: PyHieraKeyModels instance for creating key instances.
        """
        self._key_models = key_models
        self._keys: dict[str, PyHieraKeyBase] = {}
        logger.debug("Initialized keys registry")

    @property
    def keys(self) -> dict[str, PyHieraKeyBase]:
        """Get dictionary of registered keys.

        Returns:
            Dictionary mapping key names to PyHieraKeyBase instances.

        Note:
            Returns a copy to prevent external mutation of internal state.
        """
        return self._keys.copy()

    def add(self, key: str, hiera_key: str):
        """Register a new key instance.

        Args:
            key: Unique identifier for the key.
            hiera_key: Model name to instantiate (from key_models registry).

        Raises:
            PyHieraError: If hiera_key model not found in registry.
        """
        model_class = self._key_models.get(hiera_key)
        self._keys[key] = model_class()
        logger.info(f"Added key: {key} (model: {hiera_key})")

    def delete(self, key: str):
        """Remove a key from the registry.

        Args:
            key: Key identifier to remove.

        Raises:
            PyHieraError: If key not found in registry.
        """
        try:
            del self._keys[key]
            logger.info(f"Deleted key: {key}")
        except KeyError:
            raise PyHieraError(f"Key {key} not found")

    def validate(
        self,
        key: str,
        data: Any,
        sources: Optional[list[PyHieraModelBackendData]] = None,
    ) -> PyHieraModelDataBase:
        """Validate data against a registered key's model.

        Args:
            key: Key identifier for validation.
            data: Data to validate (can be any type).
            sources: Optional list of backend data sources for tracking.

        Returns:
            Validated PyHieraModelDataBase instance.

        Raises:
            PyHieraError: If key not found or data validation fails.
        """
        try:
            if sources:
                return self._keys[key].model(data=data, sources=sources)
            else:
                return self._keys[key].model(data=data)
        except KeyError:
            raise PyHieraError(f"Key {key} not found")
        except (ValueError, ValidationError) as err:
            raise PyHieraError(f"Invalid data for key {key}: {err}")


class PyHieraBackendsBase:
    """Registry for managing data backends.

    Backends provide hierarchical data storage and retrieval from various sources
    (e.g., YAML files, databases). Backends are sorted by priority for lookup order.

    Attributes:
        backends: List of backends sorted by priority (lower number = higher priority).
    """

    def __init__(self):
        """Initialize backends registry."""
        self._backends_list: list[PyHieraBackendBase] = []
        self._backends_dict: dict[str, PyHieraBackendBase] = {}
        logger.debug("Initialized backends registry")

    @property
    def backends(self) -> list[PyHieraBackendBase]:
        """Get list of backends sorted by priority.

        Returns:
            List of PyHieraBackendBase instances sorted by priority.
        """
        return self._backends_list

    def add(self, backend: PyHieraBackendBase):
        """Register a new backend.

        Args:
            backend: PyHieraBackendBase instance to register.

        Raises:
            PyHieraError: If backend identifier already exists or priority conflicts.
        """
        if backend.identifier in self._backends_dict:
            raise PyHieraError(
                f"Backend with identifier '{backend.identifier}' already exists"
            )
        for _backend in self._backends_dict.values():
            if _backend.priority == backend.priority:
                raise PyHieraError(
                    f"Backend '{backend.identifier}' cannot use priority {backend.priority} "
                    f"(already used by '{_backend.identifier}')"
                )
        self._backends_dict[backend.identifier] = backend
        self._recreate_list()
        logger.info(
            f"Added backend: {backend.identifier} (priority={backend.priority})"
        )

    def delete(self, identifier: str):
        """Remove a backend from the registry.

        Args:
            identifier: Backend identifier to remove.

        Raises:
            PyHieraError: If backend not found in registry.
        """
        try:
            del self._backends_dict[identifier]
            self._recreate_list()
            logger.info(f"Deleted backend: {identifier}")
        except KeyError:
            raise PyHieraError(f"Backend with identifier {identifier} not found")

    def get(self, identifier: str) -> PyHieraBackendBase:
        """Retrieve a backend from the registry.

        Args:
            identifier: Backend identifier to retrieve.

        Returns:
            PyHieraBackendBase instance.

        Raises:
            PyHieraError: If backend not found in registry.
        """
        try:
            return self._backends_dict[identifier]
        except KeyError:
            raise PyHieraError(f"Backend {identifier} not found")

    def _recreate_list(self):
        """Recreate and sort the backends list by priority.

        Called internally after adding or removing backends.
        """
        self._backends_list = list(self._backends_dict.values())
        self._backends_list.sort(key=lambda backend: backend.priority)


class PyHieraBackendsSync(PyHieraBackendsBase):
    """Synchronous backends registry (type-specific variant)."""

    def __init__(self):
        """Initialize synchronous backends registry."""
        super().__init__()
        self._backends_list: list[PyHieraBackendSync] = []
        self._backends_dict: dict[str, PyHieraBackendSync] = {}


class PyHieraBackendsAsync(PyHieraBackendsBase):
    """Asynchronous backends registry (type-specific variant)."""

    def __init__(self):
        """Initialize asynchronous backends registry."""
        super().__init__()
        self._backends_list: list[PyHieraBackendAsync] = []
        self._backends_dict: dict[str, PyHieraBackendAsync] = {}

    @property
    def backends(self) -> list[PyHieraBackendAsync]:
        """Get list of async backends sorted by priority.

        Returns:
            List of PyHieraBackendAsync instances sorted by priority.
        """
        return self._backends_list

    def get(self, identifier: str) -> PyHieraBackendAsync:
        """Retrieve an async backend from the registry.

        Args:
            identifier: Backend identifier to retrieve.

        Returns:
            PyHieraBackendAsync instance.

        Raises:
            PyHieraError: If backend not found in registry.
        """
        try:
            return self._backends_dict[identifier]
        except KeyError:
            raise PyHieraError(f"Backend {identifier} not found")


class PyHieraBase:
    """Base class for PyHiera hierarchical configuration management.

    This abstract base class provides the core functionality for managing
    hierarchical configuration data with multiple backends, keys, and models.

    Subclasses must implement:
        - key_data_add: Add data to a backend
        - key_data_get: Retrieve data (first match)
        - key_data_get_merge: Retrieve and merge data across hierarchy

    Attributes:
        keys: Dictionary of registered keys.
        key_models: Dictionary of registered key models.
    """

    def __init__(self):
        """Initialize PyHiera base instance."""
        self._key_models = PyHieraKeyModels()
        self._keys = PyHieraKeys(self._key_models)
        self._backends = PyHieraBackendsBase()
        logger.info("Initialized PyHiera base instance")

    @property
    def keys(self) -> dict[str, PyHieraKeyBase]:
        """Get dictionary of registered keys.

        Returns:
            Dictionary mapping key names to PyHieraKeyBase instances.
        """
        return self._keys.keys

    @property
    def key_models(self) -> dict[str, type[PyHieraKeyBase]]:
        """Get dictionary of registered key models.

        Returns:
            Dictionary mapping model names to PyHieraKeyBase subclasses.
        """
        return self._key_models.models

    def key_model_add(self, key: str, model: type[PyHieraKeyBase]):
        """Register a new key model type.

        Args:
            key: Unique identifier for the model.
            model: PyHieraKeyBase subclass to register.
        """
        self._key_models.add(key, model)

    def key_model_delete(self, key: str):
        """Remove a key model from the registry.

        Args:
            key: Model identifier to remove.
        """
        self._key_models.delete(key)

    def backend_add(self, backend: PyHieraBackendSync):
        """Register a new backend.

        Args:
            backend: PyHieraBackendSync instance to register.
        """
        self._backends.add(backend)

    def backend_delete(self, identifier: str):
        """Remove a backend from the registry.

        Args:
            identifier: Backend identifier to remove.
        """
        self._backends.delete(identifier)

    def key_add(self, key: str, hiera_key: str):
        """Register a new key instance.

        Args:
            key: Unique identifier for the key.
            hiera_key: Model name to instantiate.
        """
        self._keys.add(key, hiera_key)

    def key_delete(self, key: str):
        """Remove a key from the registry.

        Args:
            key: Key identifier to remove.
        """
        self._keys.delete(key)

    def key_data_validate(
        self,
        key: str,
        data: Any,
        sources: Optional[list[PyHieraModelBackendData]] = None,
    ) -> PyHieraModelDataBase:
        """Validate data against a key's model.

        Args:
            key: Key identifier for validation.
            data: Data to validate (can be any type).
            sources: Optional list of backend data sources for tracking.

        Returns:
            Validated PyHieraModelDataBase instance.
        """
        return self._keys.validate(key, data, sources=sources)

    def key_data_add(
        self,
        backend_identifier: str,
        key: str,
        data: Any,
        level: str,
        facts: dict[str, str],
    ):
        """Add data to a backend (abstract method).

        Args:
            backend_identifier: Backend identifier to add data to.
            key: Key name for the data.
            data: Data to add (will be validated against key's model).
            level: Hierarchy level string (may contain {fact} placeholders).
            facts: Dictionary of facts for expanding level string.

        Raises:
            NotImplementedError: Must be implemented by subclass.
        """
        raise NotImplementedError

    def key_data_get(
        self,
        key: str,
        facts: dict[str, str],
        include_sources: bool = True,
    ) -> PyHieraModelDataBase:
        """Retrieve data for a key (abstract method).

        Returns the first match found when traversing backends by priority.

        Args:
            key: Key name to retrieve.
            facts: Dictionary of facts for hierarchy level matching.
            include_sources: Whether to include source tracking information.

        Returns:
            PyHieraModelDataBase with the retrieved data.

        Raises:
            NotImplementedError: Must be implemented by subclass.
        """
        raise NotImplementedError

    def key_data_get_merge(
        self,
        key: str,
        facts: dict[str, str],
        include_sources: bool = True,
    ) -> PyHieraModelDataBase:
        """Retrieve and deep-merge data for a key (abstract method).

        Merges all matching data across the hierarchy, with more specific
        levels overriding less specific ones.

        Args:
            key: Key name to retrieve and merge.
            facts: Dictionary of facts for hierarchy level matching.
            include_sources: Whether to include source tracking information.

        Returns:
            PyHieraModelDataBase with the merged data.

        Raises:
            NotImplementedError: Must be implemented by subclass.
        """
        raise NotImplementedError

    def _key_data_get_merge(self, update, result):
        """Deep merge update into result dictionary.

        Args:
            update: Dictionary to merge from.
            result: Dictionary to merge into.

        Returns:
            The merged result dictionary.

        Note:
            - Dicts are recursively merged
            - Lists are extended (concatenated)
            - Sets are updated (union)
            - Other values are overridden
        """
        for key, value in update.items():
            if isinstance(value, dict):
                self._key_data_get_merge(value, result.setdefault(key, {}))
            elif isinstance(value, list):
                if key in result:
                    result[key].extend(value)
                else:
                    result[key] = value.copy()  # Create copy to avoid mutation
            elif isinstance(value, set):
                if key in result:
                    result[key].update(value)
                else:
                    result[key] = value.copy()  # Create copy to avoid mutation
            else:
                result[key] = value
        return result


class PyHieraAsync(PyHieraBase):
    """Asynchronous PyHiera implementation.

    Provides async/await interface for hierarchical configuration management.
    All data operations are asynchronous and can be awaited.
    """

    def __init__(self):
        """Initialize PyHieraAsync instance."""
        super().__init__()
        self._backends = PyHieraBackendsAsync()
        logger.info("Initialized PyHieraAsync instance")

    def backend_add(self, backend: PyHieraBackendAsync):
        """Register an asynchronous backend.

        Args:
            backend: PyHieraBackendAsync instance to register.
        """
        self._backends.add(backend)

    async def key_data_add(
        self,
        backend_identifier: str,
        key: str,
        data: Any,
        level: str,
        facts: dict[str, str],
    ):
        data = self.key_data_validate(key, data)
        backend = self._backends.get(backend_identifier)
        await backend.key_data_add(key, data, level, facts)

    async def key_data_get(
        self,
        key: str,
        facts: dict[str, str],
        include_sources: bool = True,
    ) -> PyHieraModelDataBase:
        if key not in self.keys:
            raise PyHieraError(f"Key {key} not found")
        for backend in self._backends.backends:
            data = await backend.key_data_get(key, facts)
            if data:
                if include_sources:
                    return self.key_data_validate(key, data[0].data, sources=[data[0]])
                else:
                    return self.key_data_validate(key, data[0].data)
        raise PyHieraBackendError("No data found")

    async def key_data_get_merge(
        self,
        key: str,
        facts: dict[str, str],
        include_sources: bool = True,
    ) -> PyHieraModelDataBase:
        if key not in self.keys:
            raise PyHieraError(f"Key {key} not found")

        data_points = []
        for backend in self._backends.backends:
            _data_points = await backend.key_data_get(key, facts)
            if _data_points:
                for data_point in _data_points:
                    if not isinstance(data_point.data, dict):
                        raise PyHieraBackendError(
                            f"Invalid data for key {key}, expected dict, got: {data_point.data}"
                        )
                    validated = self.key_data_validate(key, data_point.data)
                    # Convert to dict for merging (handles Pydantic models)
                    if hasattr(validated.data, 'model_dump'):
                        data_point.data = validated.data.model_dump()
                    else:
                        data_point.data = validated.data
                    data_points.append(data_point)

        if not data_points:
            raise PyHieraBackendError("No data found")

        merged_data = {}
        for data_point in reversed(data_points):
            merged_data = self._key_data_get_merge(data_point.data, merged_data)

        if include_sources:
            return self.key_data_validate(key, merged_data, sources=data_points)
        else:
            return self.key_data_validate(key, merged_data)


class PyHieraSync(PyHieraBase):
    """Synchronous PyHiera implementation.

    Provides synchronous interface for hierarchical configuration management.
    All data operations are blocking and return immediately.
    """

    def __init__(self):
        """Initialize PyHieraSync instance."""
        super().__init__()
        self._backends = PyHieraBackendsSync()
        logger.info("Initialized PyHieraSync instance")

    def key_data_add(
        self,
        backend_identifier: str,
        key: str,
        data: Any,
        level: str,
        facts: dict[str, str],
    ):
        data = self.key_data_validate(key, data)
        backend = self._backends.get(backend_identifier)
        backend.key_data_add(key, data, level, facts)

    def key_data_get(
        self,
        key: str,
        facts: dict[str, str],
        include_sources: bool = True,
    ) -> PyHieraModelDataBase:
        if key not in self.keys:
            raise PyHieraError(f"Key {key} not found")
        for backend in self._backends.backends:
            data = backend.key_data_get(key, facts)
            if data:
                if include_sources:
                    return self.key_data_validate(key, data[0].data, sources=[data[0]])
                else:
                    return self.key_data_validate(key, data[0].data)
        raise PyHieraBackendError("No data found")

    def key_data_get_merge(
        self,
        key: str,
        facts: dict[str, str],
        include_sources: bool = True,
    ) -> PyHieraModelDataBase:
        if key not in self.keys:
            raise PyHieraError(f"Key {key} not found")

        data_points = []
        for backend in self._backends.backends:
            _data_points = backend.key_data_get(key, facts)
            if _data_points:
                for data_point in _data_points:
                    if not isinstance(data_point.data, dict):
                        raise PyHieraBackendError(
                            f"Invalid data for key {key}, expected dict, got: {data_point.data}"
                        )
                    validated = self.key_data_validate(key, data_point.data)
                    # Convert to dict for merging (handles Pydantic models)
                    if hasattr(validated.data, 'model_dump'):
                        data_point.data = validated.data.model_dump()
                    else:
                        data_point.data = validated.data
                    data_points.append(data_point)

        if not data_points:
            raise PyHieraBackendError("No data found")

        merged_data = {}
        for data_point in reversed(data_points):
            merged_data = self._key_data_get_merge(data_point.data, merged_data)

        if include_sources:
            return self.key_data_validate(key, merged_data, sources=data_points)
        else:
            return self.key_data_validate(key, merged_data)
