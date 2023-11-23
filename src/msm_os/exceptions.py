"""Exception classes."""
import logging


class VariableNotFound(Exception):
    """Exception raised for when a variable is not found in the dataset."""

    def __init__(self, variable_name):
        """Initialise the exception."""
        message = f"Variable '{variable_name}' not found in the dataset."
        logging.warning(message)
        super().__init__(message)


class DuplicatedAppendDimValue(Exception):
    """Exception raised for when a duplicated value is found in the append dim."""

    def __init__(self, n_dupl, append_dim, first_dupl_value, last_dupl_value):
        """Initialise the exception."""
        message = (
            f"Found {n_dupl} duplicates in the append dimension '{append_dim}'.\n"
            f"Range: {first_dupl_value}--{last_dupl_value}."
        )
        logging.warning(message)
        super().__init__(message)
