"""Module with functions to handle object store operations."""
import logging
import os
from typing import List

import numpy as np
import xarray as xr

from .object_store import ObjectStoreS3
from .sanity_cheks import (
    check_destination_exists,
    check_duplicates,
    check_variable_exists,
)


def update(
    filepaths: List[str],
    bucket: str,
    store_credentials_json: str,
    variables: List[str] | None = None,
    object_prefix: str | None = None,
    to_zarr_kwargs: dict | None = None,
) -> None:
    """
    Update a region of a Zarr object in an object store.

    Parameters
    ----------
    filepaths
        Path(s) to the the file(s) to be sent.
    bucket
        Bucket name.
    store_credentials_json
        Path to the JSON file containing the credentials for the Object Store.#
    variables
        Variables to update.
    object_prefix
        Object prefix.
    to_zarr_kwargs
        Keyword arguments to pass to the `xr.open_zarr` function.

    Returns
    -------
    None
    """
    to_zarr_kwargs = to_zarr_kwargs or {}

    # Create an ObjectStoreS3 instance
    obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)

    # Check if the bucket exists
    check_destination_exists(obj_store, bucket)

    for filepath in filepaths:
        # Rename the file name if required
        if not object_prefix:
            object_prefix = os.path.basename(filepath).rsplit(".")[0].rsplit("_", 1)[0]
            object_prefix = object_prefix.replace("_", "-").lower()

        # Open the dataset from the provided filepath
        ds_filepath = xr.open_dataset(filepath)

        # Get the list of variables to update
        variables = variables or [
            var for var in ds_filepath.variables if var not in ds_filepath.coords
        ]

        # Apply update operation for each variable in the list
        for var in variables:
            # Create a mapper and open the dataset
            dest = f"{bucket}/{var}/{object_prefix}.zarr"

            # Sanity checks
            check_variable_exists(ds_filepath, var)
            check_destination_exists(obj_store, dest)

            mapper = obj_store.get_mapper(dest)
            ds_obj_store = xr.open_zarr(mapper)
            check_variable_exists(ds_obj_store, var)

            # Define the region to update
            logging.info(f"Updating {dest}")

            filepath_time = ds_filepath.time_counter.values
            index = np.where(
                np.abs(ds_obj_store.time_counter.values - filepath_time)
                < np.timedelta64(1, "ns")
            )[0]

            if len(index) == 0:
                raise ValueError(
                    f"No matching time_counter found for {filepath_time[0]} of {filepath}"
                )

            index = int(index[0])
            region = {"time_counter": slice(index, index + 1, None)}

            # When setting `region` explicitly in to_zarr(),
            # all variables in the dataset must have
            # at least one dimension in common with the region's dimensions.
            # Hence, we drop variables that do not satisfy this requisit
            # before exporting to zarr.
            vars_to_drop = [
                var
                for var in ds_filepath.variables
                if not any(dim in region.keys() for dim in ds_filepath[var].dims)
            ]
            ds_filepath = ds_filepath.drop_vars(vars_to_drop)

            # Update the data corresponding to the specified time_counter

            ds_filepath[var].to_zarr(mapper, mode="r+", region=region)

            logging.info(f"Updated {dest} at {filepath_time[0]}")


def send(
    filepaths: List[str],
    bucket: str,
    store_credentials_json: str,
    variables: List[str] | None = None,
    send_vars_indep: bool = True,
    append_dim: str = "time_counter",
    object_prefix: str | None = None,
    to_zarr_kwargs: dict | None = None,
) -> None:
    """
    Send a Zarr file to an Object Store.

    Parameters
    ----------
    filepaths
        Path to the the file to be sent.
    bucket
        Bucket name.
    store_credentials_json
        Path to the JSON file containing the credentials for the Object Store.#
    variables
        Variables to send.
    send_vars_indep
        Send independent variables.
    append_dim
        Append dimension.
    object_prefix
        Object prefix.
    to_zarr_kwargs
        Keyword arguments to pass to the `xr.open_zarr` function.

    Returns
    -------
    None
    """
    to_zarr_kwargs = to_zarr_kwargs or {}

    # Create an ObjectStoreS3 instance
    obj_store = ObjectStoreS3(anon=False, store_credentials_json=store_credentials_json)

    # Create the bucket if it doesn't exist
    if not obj_store.exists(bucket):
        logging.info(f"Bucket '{bucket}' doesn't exist. Creating...")
        obj_store.create_bucket(bucket)

    for filepath in filepaths:
        # Open the dataset
        ds_filepath = xr.open_dataset(filepath)

        # Rename the file name if required
        if not object_prefix:
            object_prefix = os.path.basename(filepath).rsplit(".")[0].rsplit("_", 1)[0]
            object_prefix = object_prefix.replace("_", "-").lower()

        # Send data to the object store
        if send_vars_indep:
            # Get the list of variables to send
            variables = variables or [
                var for var in ds_filepath.variables if var not in ds_filepath.coords
            ]

            for var in variables:
                check_variable_exists(ds_filepath, var)

                dest = f"{bucket}/{var}/{object_prefix}.zarr"
                mapper = obj_store.get_mapper(dest)
                try:
                    check_destination_exists(obj_store, dest)
                    check_duplicates(ds_filepath, mapper, append_dim)

                    logging.info(f"Appending to {dest}")
                    ds_filepath[var].to_zarr(mapper, mode="a", append_dim=append_dim)
                except FileNotFoundError:
                    logging.info(f"Creating {dest}")
                    ds_filepath[var].to_zarr(mapper, mode="w")
        else:
            dest = f"{bucket}/{object_prefix}.zarr"
            mapper = obj_store.get_mapper(dest)

            try:
                check_destination_exists(obj_store, dest)
                check_duplicates(ds_filepath, mapper, append_dim)

                logging.info(f"Appending to {dest}")
                ds_filepath.to_zarr(mapper, mode="a", append_dim=append_dim)
            except ValueError:
                logging.info(f"Creating {dest}")
                ds_filepath.to_zarr(mapper, mode="w")


def get_files(
    bucket: str,
    store_credentials_json: str,
    object_prefix: str,
) -> List[str]:
    """
    Get the list of files in the bucket.

    Parameters
    ----------
    bucket
        Bucket name.
    store_credentials_json
        Path to the JSON file containing the credentials for the Object Store.#
    object_prefix
        Object prefix.

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
