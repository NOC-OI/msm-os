"""Exception classes."""
import logging


class VariableNotFound(Exception):
    """Exception raised for when a variable is not found in the dataset."""

    def __init__(self, variable_name):
        """Initialize the exception."""
        message = f"Variable '{variable_name}' not found in the dataset."
        logging.warning(message)
        super().__init__(message)


class DuplicatedAppendDimValue(Exception):
    """Exception raised for when a duplicated value is found in the append dim."""

    def __init__(self, append_dim, append_dim_value):
        """Initialize the exception."""
        message = (
            f"Duplicate value '{append_dim_value}' found for the "
            f"append dimension '{append_dim}' in the dataset."
        )
        logging.warning(message)
        super().__init__(message)
