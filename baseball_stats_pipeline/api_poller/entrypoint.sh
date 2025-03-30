#!/bin/sh
# entrypoint.sh

# Exit immediately if a command exits with a non-zero status.
set -e

# Run the wait script first
echo "Running wait-for-minio.py..."
python wait-for-minio.py

# Now execute the main poller script, passing any arguments received by this script
echo "Starting poller.py with arguments: $@"
exec python poller.py "$@"
