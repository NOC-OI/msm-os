"""Module with sanity checks."""
import hashlib
import numpy as np
import xarray as xr
from fsspec.mapping import FSMap
from typing import List

from .exceptions import DuplicatedAppendDimValue, VariableNotFound, ExpectedAttrsNotFound, DimensionMismatch, CheckSumMismatch


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



def check_data_integrity(
    mapper: FSMap,
    var: str,
    append_dim: str,
    test_list: List[str] = None
) -> None:
    """
    
    Update/replace the object store with new data.

    Parameters
    ----------
    mapper
        The object store interface.
    var
        The variable to check.
    append_dim
        The name of the dimension to check for duplicates.
    test_list
        List of tests to perform. Default is ["metadata", "checksum"].
    """
    if test_list is None:
        test_list = ["metadata", "checksum"]

    ds_obj_store = xr.open_zarr(mapper)
    check_variable_exists(ds_obj_store, var)
    
    for test in test_list:
        if test == 'metadata':
            validate_dimensions(ds_obj_store)
            validate_variables(ds_obj_store)
        if test == "checksum":
            validate_checksum(ds_obj_store, var, append_dim)

def validate_dimensions(ds_obj_store: xr.Dataset):
    """
    Validates the dimensions of the dataset, ensuring they match expectations based on metadata.

    Parameters
    ----------
    ds_obj_store : xr.Dataset
        The dataset loaded from the object store.
    """
    for dim, size in ds_obj_store.dims.items():
        # Get the expected size from the attribute
        expected_size = ds_obj_store[dim].attrs.get('expected_size', None)
        if expected_size is None:
            raise ExpectedAttrsNotFound('expected_size')

        # Compare the expected size with the actual size
        if size != expected_size:
            raise DimensionMismatch(dim, size, expected_size)

def validate_variables(ds_obj_store: xr.Dataset):
    """
    Audit variables of the dataset.

    Parameters
    ----------
    ds_obj_store : xr.Dataset
        Dataset loaded from the Zarr store.

    """
    expected_variables = ds_obj_store.attrs.get('expected_variables', None)

    if expected_variables is None:
        raise ExpectedAttrsNotFound('expected_variables')

    for var in expected_variables:
        if var not in ds_obj_store.variables:
            raise VariableNotFound(var)

def validate_checksum(ds_obj_store: xr.Dataset, var: str, append_dim: str):
    """
    Validate the checksum of the dataset.

    Parameters
    ----------
    ds_obj_store : xr.Dataset
        Dataset loaded from the Zarr store.
    var
        The variable to check.
    append_dim
        The name of the dimension to check for duplicates.
    """
    old_chunks = ds_obj_store.attrs.get('calculated_chunks', None)
    all_chunks = ds_obj_store.chunks[append_dim]
    if old_chunks is None:
        new_chunks = all_chunks
    else:
        new_chunks = []
        for idx, value in enumerate(old_chunks):
            if value != all_chunks[idx]:
                new_chunks.append(idx)
        if len(all_chunks) > len(old_chunks):
            new_chunks.append(list(range(len(old_chunks), len(list(range(len(all_chunks)))))))
    for new_chunk in new_chunks:
        specific_chunk = ds_obj_store.isel({append_dim: new_chunk})
        if specific_chunk is not None:
            if specific_chunk.sizes[append_dim] != new_chunk:
                raise DimensionMismatch(append_dim, new_chunk, specific_chunk.sizes[append_dim])
            # if np.isnan(specific_chunk).any():
            #     print("Chunk contains NaN values.")
            expected_checksum = specific_chunk.attrs.get("expected_checksum", None)
            if expected_checksum:
                data_bytes = specific_chunk[var].values.tobytes()
                actual_checksum = np.frombuffer(data_bytes, dtype=np.uint32).sum()
                if actual_checksum != expected_checksum:
                    raise CheckSumMismatch(new_chunk, expected_checksum, actual_checksum)
