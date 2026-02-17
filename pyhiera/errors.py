"""Custom exceptions for PyHiera."""


class PyHieraError(Exception):
    """Base exception for all PyHiera errors.

    All PyHiera-specific exceptions inherit from this class, allowing
    users to catch all library errors with a single except clause.
    """

    pass


class PyHieraBackendError(PyHieraError):
    """Exception for backend-related errors.

    Raised when backends encounter issues such as:
    - Failed data retrieval
    - Invalid hierarchy configuration
    - Missing data
    """

    pass
