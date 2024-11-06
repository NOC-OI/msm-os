#!/bin/bash
# Script to send a batch of files to the object store using msm_os
# Originally created by:
#   - Joao Morado (22/11/2023)
# Modified by:
#   - 32677

# Usage:
# [nohup] ./send_batch.sh > send_batch.output 2> send_batch.errors

# User inputs
CREDENTIALS=/home/users/tobfer/credentials.json
BUCKET=tobias-chunk-test20
GLOB_EXPR=/work/scratch-nopw2/tobfer/CYLC_OUTPUTS/M71/M71/*/
MSM_OS_OUPUT=noc_os
CHUNK_STRATEGY='{"time_counter": 1, "x": 720, "y": 360}'
JOB_CONFIG='{"type": "threads", "num_threads": 16}'

# ----------------------------------------------------------------------------- #
#                                                                               #
#                   Send a batch of files to the object store                   #
#                                                                               #
# ----------------------------------------------------------------------------- #

# Get start time for the entire script
start_time_script=$(date +%s)

# Get file list
file_list=$(find $GLOB_EXPR -maxdepth 2 -type f -name "*.nc" | sort)

# Check if OUTPUT directory exists and that it contains NetCDF files
if [ ! -z "${file_list}" ]; then
    echo "Error: No .nc files found in OUTPUT directory"
    exit 1
fi

total_files=$(echo "$file_list" | wc -l)
source activate /home/users/tobfer/cylc-run/M9/M9/etc/miniconda/envs/portable_env_cylc
counter=0
for file in $file_list; do
    start_time_iteration=$(date +%s)

    ((counter++))
    percentage=$((counter * 100 / total_files))

    echo -e "Progress: $percentage %"
    echo -e "Sending ${file}"
    msm_os send -f ${file} -c ${CREDENTIALS} -b ${BUCKET} -cs "${CHUNK_STRATEGY}" -j "${JOB_CONFIG}" -si >> ${MSM_OS_OUPUT}.output 2>> ${MSM_OS_OUPUT}.errors


    # Get end time for the current iteration
    end_time_iteration=$(date +%s)
    iteration_duration=$((end_time_iteration - start_time_iteration))

    echo -e "Time taken for ${file}: $iteration_duration seconds"

done

# Get end time for the entire script
end_time_script=$(date +%s)
total_duration=$((end_time_script - start_time_script))

echo -e "\nTransfer completed!"
echo -e "Total time taken for the entire script: $total_duration seconds"
