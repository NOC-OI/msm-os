"""Module with object store handlers."""
import logging
import os
from typing import Any, List, Optional

import numpy as np
import xarray as xr

from .exceptions import (
    DuplicatedAppendDimValue,
    ExpectedAttrsNotFound,
    DimensionMismatch,
    CheckSumMismatch,
)
from .object_store import ObjectStoreS3
from .sanity_cheks import (
    check_destination_exists,
    check_duplicates,
    check_variable_exists,
    check_data_integrity,
)

try:
    from dask.distributed import Client
except ImportError:
    logging.warning(
        "Dask is not installed. Please install it to use parallel features."
    )

def update(
    filepaths: List[str],
    bucket: str,
    store_credentials_json: str,
    variables: Optional[List[str]] = None,
    append_dim: str = "time_counter",
    object_prefix: Optional[str] = None,
    to_zarr_kwargs: Optional[dict] = None,
) -> None:
    """
    Update/replace the object store with new data.

    Parameters
    ----------
    filepaths
        List of filepaths to the datasets to be updated.
    bucket
        Name of the bucket in the object store.
    store_credentials_json
        Path to the JSON file containing the object store credentials.
    variables
        List of variables to be updated. If None, all variables will be updated, by default None.
    append_dim
        Name of the append dimension, by default "time_counter".
    object_prefix :
        Prefix to be added to the object names in the object store, by default None.
    to_zarr_kwargs
        Additional keyword arguments passed to xr.Dataset.to_zarr(), by default None.
    """
    to_zarr_kwargs = to_zarr_kwargs or {}

    obj_store = ObjectStoreS3(anon=False,
                              store_credentials_json=store_credentials_json)
    check_destination_exists(obj_store, bucket)

    for filepath in filepaths:
        logging.info("Updating using %s", filepath)
        object_prefix = _get_object_prefix(filepath, object_prefix)

        ds_filepath = xr.open_dataset(filepath)
        variables = _get_update_variables(ds_filepath, variables)

        for var in variables:
            dest = f"{bucket}/{object_prefix}/{var}.zarr"

            check_variable_exists(ds_filepath, var)
            check_destination_exists(obj_store, dest)

            mapper = obj_store.get_mapper(dest)
            ds_obj_store = xr.open_zarr(mapper)
            check_variable_exists(ds_obj_store, var)

            _update_data(ds_filepath, ds_obj_store, var, append_dim, mapper)


def send(
    filepaths: List[str],
    bucket: str,
    store_credentials_json: str,
    variables: Optional[List[str]] = None,
    send_vars_indep: bool = True,
    append_dim: str = "time_counter",
    object_prefix: Optional[str] = None,
    client: Optional[Client] = None,
    to_zarr_kwargs: Optional[dict] = None,
) -> None:
    """
    Send data to the object store.

    Parameters
    ----------
    filepaths
        List of filepaths to the datasets to be sent.
    bucket
        Name of the bucket in the object store.
    store_credentials_json
        Path to the JSON file containing the object store credentials.
    variables
        List of variables to send. If None, all variables will be sent, by default None.
    send_vars_indep
        Whether to send variables as separate objects, by default True.
    append_dim
        Name of the append dimension, by default "time_counter".
    object_prefix
        Prefix to be added to the object names in the object store, by default None.
    client
        Dask client, by default None.
    to_zarr_kwargs
        Additional keyword arguments passed to xr.Dataset.to_zarr(), by default None.
    """
    to_zarr_kwargs = to_zarr_kwargs or {}

    obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)

    if not obj_store.exists(bucket):
        logging.info("Bucket '%s' doesn't exist. Creating...", bucket)

    for filepath in filepaths:
        logging.info("Sending %s", filepath)
        ds_filepath = xr.open_dataset(filepath, chunks="auto")

        prefix = _get_object_prefix(filepath, object_prefix)

        _send_data_to_store(
            obj_store,
            bucket,
            ds_filepath,
            prefix,
            variables,
            append_dim,
            send_vars_indep,
            client,
            to_zarr_kwargs,
        )


def _get_object_prefix(filepath: str, object_prefix: Optional[str]) -> str:
    """
    Get the object prefix from the filepath.

    Note
    ----
    Change this function if required.

    Parameters
    ----------
    filepath
        Filepath to the dataset.
    object_prefix
        Prefix to be added to the object names in the object store.

    Returns
    -------
    str
        The object prefix.
    """
    if not object_prefix:
        str_components = os.path.basename(filepath).split("_")

        if str_components[2] == "grid":
            object_prefix = str_components[3] + str_components[1]
        else:
            object_prefix = str_components[2] + str_components[1]

    return object_prefix


def _get_update_variables(ds_filepath: xr.Dataset, variables: List[str]) -> List[str]:
    """
    Get the variables to update.

    Parameters
    ----------
    ds_filepath
        Filepath to the dataset.
    variables
        List of variables to update. If None, all variables will be updated, by default None.

    Returns
    -------
    List[str]
        The list of variables to update.
    """
    variables = variables or [
        var for var in ds_filepath.variables if var not in ds_filepath.coords
    ]
    return variables


def _update_data(
    ds_filepath: xr.Dataset,
    ds_obj_store: xr.Dataset,
    var: str,
    append_dim: str,
    mapper: Any,
) -> None:
    """
    Update the data in the object store.

    Parameters
    ----------
    ds_filepath
        Filepath to the local dataset.
    ds_obj_store
        Dataset in the object store.
    var
        Variable to be updated.
    append_dim
        Name of the append dimension.
    mapper
        Object store mapper.
    """

    try:
        check_duplicates(ds_filepath, ds_obj_store, append_dim)
    except DuplicatedAppendDimValue:
        logging.info("Updating %s", mapper.root)
        # Define region to write to
        dupl = np.where(np.isin(ds_obj_store[append_dim], ds_filepath[append_dim]))
        dupl_max = np.max(dupl) + 1
        dupl_min = np.min(dupl)
        region = {append_dim: slice(dupl_min, dupl_max, None)}

        # Write to zarr
        vars_to_drop = [
            var
            for var in ds_filepath.variables
            if not any(dim in region.keys() for dim in ds_filepath[var].dims)
        ]
        ds_filepath = ds_filepath.drop_vars(vars_to_drop)
        ds_filepath[var].to_zarr(mapper, mode="r+", region=region)
        logging.info("Updated %s", mapper.root)

        return

    logging.info("Skipping %s because region not found in object store", mapper.root)


def _send_variable(
    ds_filepath: xr.Dataset,
    obj_store: ObjectStoreS3,
    var: str,
    bucket: str,
    object_prefix: str,
    append_dim: str,
) -> None:
    """
    Send a single variable to the object store.

    Parameters
    ----------
    ds_filepath
        Filepath to the local dataset.
    obj_store
        Object store.
    var
        Variable to be sent.
    bucket
        Name of the bucket in the object store.
    object_prefix
        Prefix to be added to the object names in the object store.
    append_dim
        Name of the append dimension.
    """
    check_variable_exists(ds_filepath, var)

    dest = f"{bucket}/{object_prefix}/{var}.zarr"
    mapper = obj_store.get_mapper(dest)
    try:
        check_destination_exists(obj_store, dest)

        if append_dim not in ds_filepath[var].dims:
            logging.info(
                "Skipping %s because %s is not in the dimensions of %s",
                dest,
                append_dim,
                var
            )
            return

        logging.info("Appending to %s along the %s dimension", dest, append_dim)

        try:
            ds_obj_store = xr.open_zarr(mapper)
            check_duplicates(ds_filepath, ds_obj_store, append_dim)

            # Calculate expected size, variables, chunks and checksum
            ds_filepath = _calculate_metadata(ds_obj_store,
                                              ds_filepath,
                                              var,
                                              append_dim)

            # Append the variable to the object store
            ds_filepath[var].to_zarr(mapper, mode="a", append_dim=append_dim)

        except DuplicatedAppendDimValue:
            logging.info(
                "Skipping %s due to duplicate values in the append dimension",
                dest
            )

        try:
            # Check data integrity
            logging.info("Checking data integrity for %s", dest)
            check_data_integrity(mapper, var, append_dim)
            logging.info("Data integrity check passed for %s", dest)
            remove_latest_version(obj_store, bucket, object_prefix, var)
        except (ExpectedAttrsNotFound, DimensionMismatch, CheckSumMismatch) as error:
            if isinstance(error, ExpectedAttrsNotFound):
                error_msg = "missing expected attributes in the metadata"
            elif isinstance(error, DimensionMismatch):
                error_msg = "dimension mismatch"
            elif isinstance(error, CheckSumMismatch):
                error_msg = "checksum mismatch"

            logging.info(
                "Error found while trying to update file %s: %s",
                dest,
                error_msg
            )
            logging.info(
                "Object store object %s will be rolled back to previous version",
                dest
            )
            rollback_object(obj_store, bucket, object_prefix, var)

    except FileNotFoundError:
        logging.info("Creating %s", dest)
        ds_filepath[var].to_zarr(mapper, mode="w")

def _calculate_metadata(ds_obj_store: xr.Dataset,
                        ds_filepath: xr.Dataset,
                        var: str,
                        append_dim: str) -> xr.Dataset:
    """
    Calculate metadata for the dataset.

    Parameters
    ----------
    ds_obj_store : xr.Dataset
        The dataset to which the variable will be appended.
    ds_filepath : xr.Dataset
        The dataset that will be appended.
    var : str
        The name of the variable being appended.
    append_dim : str
        The name of the dimension along which the variable is being appended.

    Returns
    -------
    xr.Dataset
        The dataset with the calculated metadata.
    """

    # Calculate expected size for the dimension
    expected_size = _calculate_expected_dimension_size(ds_obj_store, ds_filepath, var, append_dim)
    ds_filepath[append_dim].attrs['expected_size'] = expected_size

    # Calculate expected variables for the dataset
    expected_variables = list(set(list(ds_obj_store.keys()) + list(ds_filepath.keys())))
    ds_filepath.attrs['expected_variables'] = expected_variables

    # Calculate chunks fo the object store dataset
    ds_filepath.attrs['calculated_chunks'] = ds_obj_store.chunks[append_dim]

    # Calculate checksum for the variable
    data_bytes = ds_filepath[var].values.tobytes()
    expected_checksum = np.frombuffer(data_bytes, dtype=np.uint32).sum()
    ds_filepath.attrs['expected_checksum'] = expected_checksum

    return ds_filepath


def _calculate_expected_dimension_size(ds_obj_store: xr.Dataset,
                                       ds_filepath: xr.Dataset,
                                       var: str,
                                       append_dim: str) -> int:
    """
    Calculate the expected size for the specified dimension based on the current
    dataset and variable.

    Parameters
    ----------
    ds_obj_store : xr.Dataset
        The dataset to which the variable will be appended.
    ds_filepath : xr.Dataset
        The dataset that will be appended.
    var : str
        The name of the variable being appended.
    append_dim : str
        The name of the dimension along which the variable is being appended.

    Returns
    -------
    int
        The expected size for the specified dimension.
    """
    if append_dim == "time_counter":
        current_size = len(ds_obj_store[append_dim])
        append_size = len(ds_filepath[append_dim])
        expected_size = current_size + append_size
    elif append_dim in ds_filepath.dims:
        expected_size = len(ds_filepath[append_dim])
    else:
        expected_size = len(ds_filepath[var].dims[append_dim])

    return expected_size

def rollback_object(obj_store: ObjectStoreS3,
                    bucket,
                    object_prefix,
                    var):
    """
    Rollback the object to the previous version in the S3 bucket.

    Parameters
    ----------
    obj_store
        Object store to be used.
    bucket
        Name of the bucket in the object store.
    object_prefix
        Prefix to be added to the object names in the object store.
    var
        Name of the variable to be rolled back.
    """

    # List object versions
    versions = obj_store.versions(bucket, prefix=f"{object_prefix}/{var}.zarr")
    dest = f"{bucket}/{object_prefix}/{var}.zarr"

    # Retrieve the previous version (if available)
    if len(versions) > 1:
        previous_version_id = versions[-2]['VersionId']
        obj_store.copy(f"s3://{dest}?versionId={previous_version_id}", f"{dest}")
        logging.info("Rolled back %s to the previous version", dest)
    else:
        logging.info("No previous version found for %s", dest)

def remove_latest_version(obj_store: ObjectStoreS3,
                          bucket,
                          object_prefix,
                          var):
    """
    Remove the latest version of the object from the S3 bucket.

    Parameters
    ----------
    obj_store
        Object store to be used.
    bucket
        Name of the bucket in the object store.
    object_prefix
        Prefix to be added to the object names in the object store.
    var
        Name of the variable to be rolled back.
    """
    # List object versions
    versions = obj_store.versions(bucket, prefix=f"{object_prefix}/{var}.zarr")
    dest = f"{bucket}/{object_prefix}/{var}.zarr"


    # Delete the latest version (if available)
    if versions:
        latest_version_id = versions[0]['VersionId']
        obj_store.rm(f"s3://{dest}?versionId={latest_version_id}")
        logging.info("Deleted the latest version of %s", dest)
    else:
        logging.info("No version found for %s", dest)

def _send_data_to_store(
    obj_store: ObjectStoreS3,
    bucket: str,
    ds_filepath: xr.Dataset,
    object_prefix: str,
    variables: List[str],
    append_dim: str,
    send_vars_indep: bool,
    client: Client,
    to_zarr_kwargs: dict,
) -> None:
    """
    Send data to the object store.

    Parameters
    ----------
    obj_store
        Object store to be used.
    bucket
        Name of the bucket in the object store.
    ds_filepath
        Dataset to be sent.
    object_prefix
        Prefix to be added to the object names in the object store.
    variables
        List of variables to send. If None, all variables will be sent.
    append_dim
        Name of the append dimension.
    send_vars_indep
        Whether to send variables as separate objects.
    client
        Dask client.
    to_zarr_kwargs
        Additional keyword arguments passed to xr.Dataset.to_zarr(), by default None.
    """
    #TODO: Add support for parallel sending
    #TODO: Add support for zarr metadata
    
    # See https://stackoverflow.com/questions/66769922/concurrently-write-xarray-datasets-to-zarr-how-to-efficiently-scale-with-dask
    if send_vars_indep:
        variables = _get_update_variables(ds_filepath, variables)

        if client:
            futures = []
            for var in variables:
                futures.append(
                    client.submit(
                        _send_variable,
                        ds_filepath,
                        obj_store,
                        var,
                        bucket,
                        object_prefix,
                        append_dim,
                    )
                )
            client.gather(futures)
        else:
            for var in variables:
                _send_variable(
                    ds_filepath, obj_store, var, bucket, object_prefix, append_dim
                )

    else:
        dest = f"{bucket}/{object_prefix}.zarr"
        mapper = obj_store.get_mapper(dest)

        try:
            check_destination_exists(obj_store, dest)
            logging.info("Appending to %s along the %s dimension", dest, append_dim)

            try:
                ds_obj_store = xr.open_zarr(mapper)
                check_duplicates(ds_filepath, ds_obj_store, append_dim)
                ds_filepath.to_zarr(mapper, mode="a", append_dim=append_dim)
            except DuplicatedAppendDimValue:
                logging.info(
                    "Skipping %s due to duplicate values in the append dimension", dest
                    )

        except FileNotFoundError:
            logging.info("Creating %s", dest)
            ds_filepath.to_zarr(mapper, mode="w")


def get_files(
    bucket: str,
    store_credentials_json: str,
) -> List[str]:
    """
    Get the list of files in the bucket.

    Parameters
    ----------
    bucket
        Bucket name.
    store_credentials_json
        Path to the JSON file containing the credentials for the Object Store.#

    Returns
    -------
    List[str]
        List of files in the bucket.
    """
    obj_store = ObjectStoreS3(anon=False,
                              store_credentials_json=store_credentials_json)
    logging.info("Getting list of files in bucket '%s'", bucket)
    for file in obj_store.ls(f"{bucket}"):
        logging.info(file)
    return obj_store.ls(f"{bucket}")
