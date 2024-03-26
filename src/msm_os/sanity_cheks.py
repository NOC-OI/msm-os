"""Module with sanity checks."""
import hashlib
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


def calculate_checksum(data_bytes: bytes) -> str:
    """
    Calculate the SHA-256 checksum of given data bytes.
    
    Parameters
    ----------
    data_bytes : bytes
        The data over which to calculate the checksum.
    
    Returns
    -------
    str
        The calculated checksum as a hexadecimal string.
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data_bytes)
    return sha256_hash.hexdigest()

def calculate_checksum_chunkwise(variable: xr.DataArray) -> str:
    """
    Calculate the SHA-256 checksum of a DataArray variable, chunk by chunk.
    
    Parameters
    ----------
    variable : xr.DataArray
        The DataArray variable for which to calculate the checksum.
    
    Returns
    -------
    str
        The final checksum as a hexadecimal string, representing the combined checksum of all chunks.
    """
    sha256_hash = hashlib.sha256()
    
    # Ensure the data is loaded as dask array to avoid loading into memory
    if isinstance(variable.data, np.ndarray):
        variable = variable.chunk()
    
    # Iterate over all chunks
    for block in variable.data.blocks:
        # Compute the checksum for each chunk and update the overall hash
        chunk_data = block.compute()
        sha256_hash.update(chunk_data.tobytes())
    
    return sha256_hash.hexdigest()

def verify_checksum(
    ds_obj_store: xr.Dataset,
    bucket: str,
    object_prefix: str,
    var: str
) -> bool:
    """
    Verify the checksum of a variable in the object store matches the pre-calculated checksum.
    
    Parameters
    ----------
    obj_store : ObjectStoreS3
        The object store interface.
    bucket : str
        The bucket name.
    object_prefix : str
        The object prefix.
    var : str
        The variable name.
        
    Returns
    -------
    bool
        True if the checksums match, False otherwise.
    """
    # Fetch the stored checksum
    stored_checksum = ds_obj_store.attrs.get(f"{var}_checksum", "")   
    recalculated_checksum = calculate_checksum(ds_obj_store[var].values.tobytes()
    
    # Compare the checksums
    return stored_checksum == recalculated_checksum


def validate_dimensions(ds_obj_store: xr.Dataset):
    """
    Validates the dimensions of the dataset, ensuring they match expectations based on metadata.

    Parameters
    ----------
    ds_obj_store : xr.Dataset
        The dataset loaded from the object store.
    """
    for dim, size in ds_obj_store.dims.items():
        # Assuming you have a way to determine the expected size for each dimension
        expected_size = get_expected_dimension_size(dim)
        if size != expected_size:
            raise ValueError(f"Dimension {dim} has size {size}, expected {expected_size}.")

def validate_variable_attributes(ds_obj_store: xr.Dataset):
    """
    Validates variable attributes such as data types and attribute values.

    Parameters
    ----------
    ds_obj_store : xr.Dataset
        The dataset loaded from the object store.
    """
    for var_name, var in ds_obj_store.variables.items():
        # Check data type
        expected_dtype = get_expected_dtype(var_name)
        if var.dtype != expected_dtype:
            raise ValueError(f"Variable {var_name} has dtype {var.dtype}, expected {expected_dtype}.")
        
        # Check other attributes like units, descriptions, etc.
        # assuming you have a way to determine expected attributes
        expected_attributes = get_expected_attributes(var_name)
        for attr_name, expected_value in expected_attributes.items():
            if var.attrs.get(attr_name) != expected_value:
                raise ValueError(f"Attribute {attr_name} for variable {var_name} is {var.attrs.get(attr_name)}, expected {expected_value}.")


def check_checksum(ds_obj_store: xr.Dataset, var: str):
    """ 
    Check if the checksum of the variable is equal to the checksum in the object store.
    
    Parameters
    ----------
    ds_obj_store : xr.Dataset
        The dataset in the object store.
    var : str
        The name of the variable to check.
    """
    
    saved_checksum = ds_obj_store.attrs.get(f"{var}_checksum", "")
    checksum_obj_store = calculate_checksum_chunkwise(ds_obj_store[var])
    if checksum != checksum_obj_store:
        logging.error(f"Checksum of {var} is not equal to the checksum in the object store")
    else:
        logging.info(f"Checksum of {var} is equal to the checksum in the object store")
