"""Module with sanity checks."""
import numpy as np
import xarray as xr
from fsspec.mapping import FSMap

from .exceptions import DuplicatedAppendDimValue, VariableNotFound


def check_duplicates(
    ds_filepath: xr.Dataset,
    ds_obj_store: xr.Dataset,
    append_dim: str,
) -> None:
    """
    Check if there are duplicates in the append dimension.

    Parameters
    ----------
    ds_filepath
        Local dataset to be sent.
    ds_obj_store
        Dataset in the object store.
    append_dim
        The name of the dimension to check for duplicates.

    Raises
    ------
    DuplicatedAppendDimValue
        If duplicates are found in the append dimension.
    """
    filepath_append_dim = ds_filepath[append_dim]

    # Number of duplicates in the append dimension
    n_dupl = np.sum(np.isin(ds_obj_store[append_dim], filepath_append_dim))

    if n_dupl == 0:
        return n_dupl
    elif n_dupl == filepath_append_dim.size:
        raise DuplicatedAppendDimValue(
            n_dupl,
            append_dim,
            filepath_append_dim.values[0],
            filepath_append_dim.values[-1],
        )
    elif n_dupl > 0:
        raise ValueError(
            f"Only found {n_dupl} duplicates in the append dimension when "
            f"there are {filepath_append_dim.size} values in the dataset."
        )
    else:
        raise NotImplementedError(
            "Found error in check_duplicates which is not implemented."
        )


def check_variable_exists(
    ds: xr.Dataset,
    var: str,
) -> None:
    """
    Check if the variable exists in the dataset.

    Parameters
    ----------
    ds
        Dataset to be checked.
    var : str
        The name of the variable to check.

    Raises
    ------
    VariableNotFound
        If the variable is not found in the dataset.
    """
    if var not in ds:
        raise VariableNotFound(var)


def check_destination_exists(
    obj_store: FSMap,
    dest: str,
) -> None:
    """
    Check if the destination exists in the object store.

    Parameters
    ----------
    obj_store
        Object store to be checked.
    dest
        The name of the destination to check.

    Raises
    ------
    FileNotFoundError
        If the destination is not found in the object store.
    """
    if not obj_store.exists(dest):
        raise FileNotFoundError(f"Destination '{dest}' doesn't exist in object store.")
