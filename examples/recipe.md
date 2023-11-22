## Updating variables in an object store

### 1. Send original variables to the object store

Use the `msm_os send` command to send the `e3t` and `zos` variables of the `eORCA025_era5` dataset to the object store. Provide the following parameters:

- `-f`: The path to the NetCDF file containing the variables.
- `-c`: The path to the JSON file containing the object store credentials.
- `-b`: The bucket name in the object store where the variables will be stored.
- `-v`: A list of variable names to send.

Repeat the command for each NetCDF file containing the variables you want to send.

```bash
msm_os send -f eORCA025_1y_grid_T_1976-1976.nc -c credentials.json -b updatetest -v e3t zos
msm_os send -f eORCA025_1y_grid_T_1977-1977.nc -c credentials.json -b updatetest -v e3t zos
```
### 2. Generate a corrected version of the file

- Rename the original NetCDF file to a backup file with the `_old.nc` suffix.
- Rename the corrected NetCDF file to the original filename.

```bash
mv eORCA025_1y_grid_T_1976-1976.nc eORCA025_1y_grid_T_1976-1976_old.nc
mv eORCA025_1y_grid_T_1976-1976_new.nc eORCA025_1y_grid_T_1976-1976.nc
```

### 3. Update the variable in the object store

Now use the `msm_os update` command to update the `e3t` variable in the object store. Provide the following parameters:

- `-f`: The path to the updated NetCDF file containing the variable.
- `-c`: The path to the JSON file containing the object store credentials.
- `-b`: The bucket name in the object store where the variable is stored.
- `-v`: The name of the variable to update.

```bash
msm_os update -f eORCA025_1y_grid_T_1976-1976.nc -c credentials.json -b updatetest -v e3t
```
