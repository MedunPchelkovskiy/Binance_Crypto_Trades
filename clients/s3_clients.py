import logging

import boto3
from decouple import config

logger = logging.getLogger(__name__)


def get_s3_client(endpoint_url=None, access_key=None, secret_key=None):

    try:
        s3_client = boto3.client(
            "S3",
            endpoint_url=endpoint_url or config("MINIO_ENDPOINT"),
            aws_access_key_id=access_key or config("MINIO_ACCESS_KEY"),
            aws_secret_access_key=secret_key or config("MINIO_SECRET_KEY"),
        )
        logger.info("S3 client initialized successfully")
        return s3_client

    except Exception:
        logger.exception("Failed to initialize S3 client")
        raise
