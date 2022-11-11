#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which load_cnosdb)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "load_cnosdb not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-cnosdb-data}
DATABASE_PORT=${DATABASE_PORT:-31007}

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

until curl http://${DATABASE_HOST}:${DATABASE_PORT}/api/v1/ping 2>/dev/null; do
    echo "Waiting for CnosDB"
    sleep 1
done

# Remove previous database
curl -X POST http://${DATABASE_HOST}:${DATABASE_PORT}/api/v1/sql \
-u "cnosdb:xx" \
-d "drop database if exists ${DATABASE_NAME} "
# Load new data
cat ${DATA_FILE} | $EXE_FILE_NAME \
                                --db-name=${DATABASE_NAME} \
                                --backoff=${BACKOFF_SECS} \
                                --workers=${NUM_WORKERS} \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --urls=http://${DATABASE_HOST}:${DATABASE_PORT}
