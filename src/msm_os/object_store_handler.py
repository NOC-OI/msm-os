"""Module with functions to handle object store operations."""
import logging
import os
from typing import List

import numpy as np
import xarray as xr

from .exceptions import VariableNotFound
from .object_store import ObjectStoreS3


def update(
    filepaths: List[str],
    bucket: str,
    store_credentials_json: str,
    variables: List[str] | None = None,
    object_prefix: str | None = None,
    to_zarr_kwargs: dict | None = None,
) -> None:
    """
    Update a region of of an object in an object store in Zarr format.

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
            if var not in ds_filepath:
                raise VariableNotFound(var)

            # Create a mapper and open the dataset
            dest = f"{bucket}/{var}/{object_prefix}.zarr"
            mapper = obj_store.get_mapper(dest)
            ds_obj_store = xr.open_zarr(mapper)
            logging.info(f"Updating {dest}")
            if var not in ds_obj_store:
                raise VariableNotFound(var)

            # Define the region to update
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
        ds = xr.open_dataset(filepath)

        # Rename the file name if required
        if not object_prefix:
            object_prefix = os.path.basename(filepath).rsplit(".")[0].rsplit("_", 1)[0]
            object_prefix = object_prefix.replace("_", "-").lower()

        # Send data to the object store
        if send_vars_indep:
            # Get the list of variables to send
            variables = variables or [var for var in ds.variables]

            for var in variables:
                if var not in ds:
                    raise VariableNotFound(var)

                dest = f"{bucket}/{var}/{object_prefix}.zarr"
                mapper = obj_store.get_mapper(dest)
                try:
                    # TODO: add check to verify if time_counter is present
                    ds[var].to_zarr(mapper, mode="a", append_dim=append_dim)
                    logging.info(f"Appended to {dest}")
                except ValueError:
                    ds[var].to_zarr(mapper, mode="w")
                    logging.info(f"Created {dest}")
        else:
            dest = f"{bucket}/{object_prefix}.zarr"
            mapper = obj_store.get_mapper(dest)
            try:
                # TODO: add check to verify if time_counter is present
                ds.to_zarr(mapper, mode="a", append_dim=append_dim)
                logging.info(f"Appended to {dest}")
            except ValueError:
                ds.to_zarr(mapper, mode="w")
                logging.info(f"Created {dest}")
