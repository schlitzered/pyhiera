"""Key models for PyHiera data validation."""

from typing import Any

from pyhiera.models import PyHieraModelDataBase
from pyhiera.models import PyHieraModelDataString
from pyhiera.models import PyHieraModelDataInt
from pyhiera.models import PyHieraModelDataFloat
from pyhiera.models import PyHieraModelDataBool


class PyHieraKeyBase:
    """Base class for PyHiera key types.

    Key types define the validation schema and data type for hierarchical data.
    Subclasses specify the Pydantic model for data validation.

    Attributes:
        description: Human-readable description of the key type.
        model: Pydantic model class for data validation.
    """

    def __init__(self):
        """Initialize base key type."""
        self._description = "Base key type (abstract)"
        self._model = PyHieraModelDataBase

    @property
    def description(self) -> str:
        """Get key type description.

        Returns:
            Human-readable description string.
        """
        return self._description

    @property
    def model(self) -> type[PyHieraModelDataBase]:
        """Get Pydantic model class for this key type.

        Returns:
            PyHieraModelDataBase subclass for validation.
        """
        return self._model

    def validate(self, data: Any) -> PyHieraModelDataBase:
        """Validate data against this key's model.

        Args:
            data: Data to validate.

        Returns:
            Validated PyHieraModelDataBase instance.

        Note:
            This method is currently unused. The code uses .model() directly.
        """
        return self._model(data=data)


class PyHieraKeyString(PyHieraKeyBase):
    """Key type for string values."""

    def __init__(self):
        """Initialize string key type."""
        super().__init__()
        self._description = "String value"
        self._model = PyHieraModelDataString


class PyHieraKeyInt(PyHieraKeyBase):
    """Key type for integer values."""

    def __init__(self):
        """Initialize integer key type."""
        super().__init__()
        self._description = "Integer value"
        self._model = PyHieraModelDataInt


class PyHieraKeyFloat(PyHieraKeyBase):
    """Key type for floating-point values."""

    def __init__(self):
        """Initialize float key type."""
        super().__init__()
        self._description = "Floating-point value"
        self._model = PyHieraModelDataFloat


class PyHieraKeyBool(PyHieraKeyBase):
    """Key type for boolean values."""

    def __init__(self):
        """Initialize boolean key type."""
        super().__init__()
        self._description = "Boolean value"
        self._model = PyHieraModelDataBool
