"""Module with object store handlers."""
import logging
import os
from typing import Any, List, Optional

import numpy as np
import xarray as xr

from .exceptions import DuplicatedAppendDimValue
from .object_store import ObjectStoreS3
from .sanity_cheks import (
    check_destination_exists,
    check_duplicates,
    check_variable_exists,
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

    obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
    check_destination_exists(obj_store, bucket)

    for filepath in filepaths:
        logging.info(f"Updating using {filepath}")
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
        logging.info(f"Bucket '{bucket}' doesn't exist. Creating...")
        obj_store.create_bucket(bucket)

    for filepath in filepaths:
        logging.info(f"Sending {filepath}")
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
        logging.info(f"Updating {mapper.root}")
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
        logging.info(f"Updated {mapper.root}")

        return

    logging.info(f"Skipping {mapper.root} because region not found in object store")


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
                f"Skipping {dest} because {append_dim} is not in the dimensions of {var}"
            )
            return

        logging.info(f"Appending to {dest} along the {append_dim} dimension")

        try:
            ds_obj_store = xr.open_zarr(mapper)
            check_duplicates(ds_filepath, ds_obj_store, append_dim)
            ds_filepath[var].to_zarr(mapper, mode="a", append_dim=append_dim)
        except DuplicatedAppendDimValue:
            logging.info(
                f"Skipping {dest} due to duplicate values in the append dimension"
            )

    except FileNotFoundError:
        logging.info(f"Creating {dest}")
        ds_filepath[var].to_zarr(mapper, mode="w")


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
        Additional keyword arguments passed to xr.Dataset.to_zarr().
    """
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
            logging.info(f"Appending to {dest} along the {append_dim} dimension")

            try:
                ds_obj_store = xr.open_zarr(mapper)
                check_duplicates(ds_filepath, ds_obj_store, append_dim)
                ds_filepath.to_zarr(mapper, mode="a", append_dim=append_dim)
            except DuplicatedAppendDimValue:
                logging.info(
                    f"Skipping {dest} due to duplicate values in the append dimension"
                )

        except FileNotFoundError:
            logging.info(f"Creating {dest}")
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
    obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)
    logging.info(f"List of files in bucket '{bucket}':")
    for file in obj_store.ls(f"{bucket}"):
        logging.info(file)
    return obj_store.ls(f"{bucket}")
