"""Exception classes."""


class VariableNotFound(Exception):
    """Exception raised for when a variable is not found in the dataset."""

    def __init__(self, variable_name):
        """Initialize the exception."""
        self.variable_name = variable_name
        super().__init__(f"Variable '{self.variable_name}' not found in the dataset.")


class DuplicatedAppendDimValue(Exception):
    """Exception raised for when a duplicated value is found in the append dim."""

    def __init__(self, append_dim, append_dim_value):
        """Initialize the exception."""
        self.append_dim = append_dim
        self.append_dim_value = append_dim_value
        super().__init__(
            f"Duplicated value '{self.append_dim_value}' found for the"
            "append dimension '{self.append_dim}' in the dataset."
        )
