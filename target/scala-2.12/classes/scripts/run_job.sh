#!/bin/bash

# Usage: ./run_job.sh <input_path> <output_path>

# Validate input arguments
if [ "$#" -ne 2 ]; then
  echo "Illegal number of parameters. Usage: ./run_job.sh <input_path> <output_path>"
  exit 1
fi

# Arguments
INPUT_PATH=$1
OUTPUT_PATH=$2

# Spark-submit parameters
APP_NAME="JobRunner"
JAR_PATH="target/scala-2.12/your-app-name-assembly-0.1.jar" # Update jar path & app name
MAIN_CLASS="com.example.runnable.JobRunner"

# Execute spark-submit with the parameters
spark-submit \
  --class "$MAIN_CLASS" \
  --master local[*] \
  --name "$APP_NAME" \
  "$JAR_PATH" \
  "$INPUT_PATH" \
  "$OUTPUT_PATH"