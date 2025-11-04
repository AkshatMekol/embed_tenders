import aioboto3
from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET
from io import BytesIO
import os

async def list_pdfs(prefix):
    pdf_keys = []
    session = aioboto3.Session()
    async with session.client("s3", region_name=AWS_REGION,
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY) as s3:
        paginator = s3.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".pdf"):
                    pdf_keys.append(key)
    return pdf_keys

async def download_pdf(key):
    session = aioboto3.Session()
    async with session.client("s3", region_name=AWS_REGION,
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY) as s3:
        obj = await s3.get_object(Bucket=S3_BUCKET, Key=key)
        return BytesIO(await obj['Body'].read())
