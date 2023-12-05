import glob
import logging
import sys

from dask.distributed import Client, LocalCluster

from msm_os.object_store_handler import send

if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout,
        format="☁  msm_os ☁  | %(levelname)10s | %(asctime)s | %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Create LocalCluster
    cluster = LocalCluster(n_workers=6, threads_per_worker=2)
    client = Client(cluster)
    print(f"Dask Dashboard Link: {client.dashboard_link}")

    # Filelist
    file_list = glob.glob("/home/joaomorado/Desktop/xarray_tests/recipe/*.nc")

    # Send files to object store
    send(
        filepaths=file_list,
        bucket="trial-msmos",
        store_credentials_json="/home/joaomorado/Desktop/xarray_tests/credentials.json",
        append_dim="time_counter",
        send_vars_indep=True,
        client=client,
    )
