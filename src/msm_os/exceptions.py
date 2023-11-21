"""Exception classes."""


class VariableNotFound(Exception):
    """Exception raised for when a variable is not found in the dataset."""

    def __init__(self, variable_name):
        """Initialize the exception."""
        self.variable_name = variable_name
        super().__init__(f"Variable '{self.variable_name}' not found in the dataset.")
