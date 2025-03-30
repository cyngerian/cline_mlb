import os
import time
import logging
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('wait-for-minio')

S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')
S3_ACCESS_KEY = os.environ.get('S3_ACCESS_KEY')
S3_SECRET_KEY = os.environ.get('S3_SECRET_KEY')
S3_REGION = os.environ.get('S3_REGION', 'us-east-1')
MAX_RETRIES = 5 # Approx 10 seconds total wait
RETRY_DELAY = 2 # seconds

logger.info(f"Waiting for MinIO/S3 service at {S3_ENDPOINT_URL or 'AWS S3 default'}...")

for attempt in range(MAX_RETRIES):
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            region_name=S3_REGION
        )
        # Use list_buckets as the readiness check
        s3_client.list_buckets()
        logger.info("MinIO/S3 service is ready.")
        exit(0) # Exit successfully
    except ClientError as e:
        logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: MinIO/S3 not ready yet ({e}). Retrying in {RETRY_DELAY} seconds...")
    except Exception as e:
        logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Unexpected error connecting to MinIO/S3 ({e}). Retrying in {RETRY_DELAY} seconds...")

    time.sleep(RETRY_DELAY)

logger.error(f"MinIO/S3 service not ready after {MAX_RETRIES} attempts.")
exit(1) # Exit with error
