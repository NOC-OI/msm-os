# msm-os package

A library to streamline the transfer, update, and deletion of Zarr files within object store environments.

## Installation

TODO.

## Usage

### Sending Files

To send a file to an object store, use the following command:

```bash
msm_os send -f eORCA025_1y_grid_T_1976-1976.nc -c credentials.json -b eorca025
```
The flags used are the following:
- `-f`: The path to the NetCDF file containing the variables.
- `-c`: The path to the JSON file containing the object store credentials.
- `-b`: The bucket name in the object store where the variables will be stored.

Since no `--prefix` is provided, the variables will be stored in `eorca025/T1y/<var>.zarr`. If a `--prefix` is provided, the variables will be stored in `eorca025/<prefix>/<var>.zarr`.

Note that a list of variables to be sent to the object store can be specified using the `-v` flag. If this is not provided, all variables will be sent.

### Updating or Replacing Files

To update the values of an existing variable in an object store, use the following command:

```bash
msm_os update -f eORCA025_1y_grid_T_1976-1976.nc -c credentials.json -b eorca025 -v e3t
```

This will locate the region with the same timestamp as given by `eORCA025_1y_grid_T_1976-1976.nc` and overwrite the values of `e3t` in the object store.

An additional flag is used in this case:

- `-v`: The name of the variable to update.

### Check data integrity

1. Implementing Data Integrity Checks
- pre-upload check
- checksum
- versioning
- metadata validation

2. Correcting Inconsistent Metadata
- create a repair tool
- retry logic
- manual check
- post upload checking

## Flags

### Mandatory Flags

| Long version | Short Version | Description |
|---|---|---|
| action | | Specify the action: `send` to send a file or `update` to update an existing object. |
| `--filepaths` | `-f` | Paths to the files to send or update. |
| `--credentials` | `-c` | Path to the JSON file containing the credentials for the object store. |
| `--bucket` | `-b` | Bucket name. |

### Optional flags

| Flag | Short Version | Description |
|---|---|---|
| `--prefix` | `-p` | Object prefix. |
| `--append_dim` | `-a` | Append dimension (default=`time_counter`). |
| `--variables` | `-v` | Variables to send. If not provided, all variables will be sent.  If set to `compact`, the variables will not be sent to separate Zarr files. |

## Credentials File

The credentials file should contain the following information:

```json
{
    "secret": "your_secret",
    "token": "your_token",
    "endpoint_url": "https://noc-msm-o.s3-ext.jc.rl.ac.uk"
}
``````
