import boto3

from decouple import config


def get_s3_client(endpoint_url=None, access_key=None, secret_key=None):
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url or config("MINIO_ENDPOINT"),
        aws_access_key_id=access_key or config("MINIO_ACCESS_KEY"),
        aws_secret_access_key=secret_key or config("MINIO_SECRET_KEY"),
    )
    return s3_client
