#!/bin/bash
# Script to send a batch of files to the object store using msm_os
# Originally created by:
#   - Joao Morado (22/11/2023)
# Modified by:
#   - 32677

# Usage:
# [nohup] ./send_batch.sh > send_batch.output 2> send_batch.errors

# User inputs
CREDENTIALS=/home/joamor/obj_store_workspace/jasmin_credentials.json
BUCKET=msm-eorca025-era5
GLOB_EXPR=/dssgfs01/scratch/atb299/NOC_NPD/simulations/eORCA025_ERA5/*/
MSM_OS_OUPUT=noc_os

# ----------------------------------------------------------------------------- #
#                                                                               #
#                   Send a batch of files to the object store                   #
#                                                                               #
# ----------------------------------------------------------------------------- #
# Get file list
file_list=$(find $GLOB_EXPR -maxdepth 2 -type f -name "*.nc" | sort)

# Check if OUTPUT directory exists and that it contains NetCDF files
if [ ! -d "OUTPUT" ] || [ ! -z "${file_list}" ]; then
    echo "Error: No .nc files found in OUTPUT directory or OUTPUT directory does not exist."
    exit 1
fi

total_files=$(echo "$file_list" | wc -l)

counter=0
for file in $file_list; do
    ((counter++))
    percentage=$((counter * 100 / total_files))

    echo -ne "Progress: ["
    for ((i = 0; i < percentage / 2; i++)); do
        echo -ne "="
    done
    echo -ne ">] $percentage% \r"

    echo -e "Sending ${file}"
    msm_os send -f ${file} -c ${CREDENTIALS} -b ${BUCKET} >> ${MSM_OS_OUPUT}.output 2>> ${MSM_OS_OUPUT}.errors
done

echo -e "\nTransfer completed!"
