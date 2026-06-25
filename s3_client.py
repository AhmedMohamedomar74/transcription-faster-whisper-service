import json
import os
import time
import logging

import boto3
from botocore.exceptions import ClientError

from config import settings

logger = logging.getLogger(__name__)

_client = None


def get_s3_client():
    global _client
    if _client is None:
        _client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
        )
    return _client


def ensure_bucket():
    s3 = get_s3_client()
    bucket = settings.s3_bucket
    max_attempts = 10
    for attempt in range(max_attempts):
        try:
            s3.create_bucket(Bucket=bucket)
            logger.info("Created S3 bucket: %s", bucket)
            return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                logger.info("S3 bucket already exists: %s", bucket)
                return
            if attempt < max_attempts - 1:
                logger.warning("Failed to create bucket (attempt %d/%d): %s", attempt + 1, max_attempts, e)
                time.sleep(3)
            else:
                raise
        except Exception as e:
            if attempt < max_attempts - 1:
                logger.warning("S3 not ready (attempt %d/%d): %s", attempt + 1, max_attempts, e)
                time.sleep(3)
            else:
                raise


def upload_file(local_path: str, bucket: str, key: str) -> str:
    s3 = get_s3_client()
    s3.upload_file(local_path, bucket, key)
    logger.info("Uploaded to s3://%s/%s (%.1fMB)", bucket, key, os.path.getsize(local_path) / 1024 / 1024)
    return key


def download_file(bucket: str, key: str, local_path: str) -> str:
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    s3 = get_s3_client()
    s3.download_file(bucket, key, local_path)
    logger.info("Downloaded s3://%s/%s -> %s", bucket, key, local_path)
    return local_path


def upload_json(data: dict, bucket: str, key: str) -> str:
    s3 = get_s3_client()
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    logger.info("Uploaded JSON to s3://%s/%s (%.1fKB)", bucket, key, len(body) / 1024)
    return key


def generate_presigned_url(bucket: str, key: str, expiration: int = 3600) -> str:
    s3 = get_s3_client()
    url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expiration,
    )
    url = url.replace(settings.s3_endpoint_url, settings.s3_public_endpoint_url)
    return url


def delete_prefix(bucket: str, prefix: str):
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    delete_keys = []
    for page in pages:
        for obj in page.get("Contents", []):
            delete_keys.append({"Key": obj["Key"]})
    if delete_keys:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_keys})
        logger.info("Deleted %d objects under s3://%s/%s", len(delete_keys), bucket, prefix)
