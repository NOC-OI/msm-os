"""Module with sanity checks."""
import numpy as np
import xarray as xr
from fsspec.mapping import FSMap

from .exceptions import DuplicatedAppendDimValue, VariableNotFound


def check_duplicates(
    ds_filepath: xr.Dataset,
    mapper: FSMap,
    append_dim: str,
) -> None:
    """
    Check if there are duplicates in the append dimension.

    Parameters
    ----------
    ds_filepath
        Local dataset to be sent.
    mapper
        The mapper for opening the remote Zarr store.
    append_dim
        The name of the dimension to check for duplicates.

    Raises
    ------
    DuplicatedAppendDimValue
        If duplicates are found in the append dimension.
    """
    ds_obj_store = xr.open_zarr(mapper)
    filepath_time = ds_filepath[append_dim].values
    index = np.where(
        np.abs(ds_obj_store[append_dim].values - filepath_time)
        < np.timedelta64(1, "ns")
    )[0]

    if len(index) > 0:
        raise DuplicatedAppendDimValue(append_dim, filepath_time[0])


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
