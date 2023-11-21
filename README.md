# msm-os package

## Installation

TODO.

## Usage

### Sending Files

To send a file to the Object Store, use the following command:

```bash
msm_os send -f eORCA025_1y_grid_T_1976-1976.nc -c credentials.json -b eorca025
```

Since no `--prefix` is provided, the variables will be stored in `eorca025/<var>/eORCA025_1y_grid_T.zarr`.

### Updating or Replacing Files

To update the values of an existing variable, use the following command:

```bash
msm_os update -f eORCA025_1y_grid_T_1976-1976.nc -c credentials.json -b eorca025 -v e3t
```

This will locate the region with the same timestamp as given by `eORCA025_1y_grid_T_1976-1976.nc` and overwrite the values of `e3t` in the object store.

## Required Flags

| Long version | Short Version | Description |
|---|---|---|
| action | | Specify the action: `send` to send a file or `update` to update an existing object. |
| `--filepaths` | `-f` | Paths to the files to send or update. |
| `--credentials` | `-c` | Path to the JSON file containing the credentials for the object store. |
| `--bucket` | `-b` | Bucket name. |

## Flag Descriptions

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
