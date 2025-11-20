import aioboto3
from io import BytesIO
from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET

# Create an async session
session = aioboto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

async def list_s3_pdfs(prefix: str):
    async with session.client("s3") as s3_client:
        paginator = s3_client.get_paginator("list_objects_v2")
        pdf_keys = []

        async for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].lower().endswith(".pdf"):
                    pdf_keys.append(obj["Key"])
        return pdf_keys

async def fetch_pdf(key: str) -> BytesIO:
    async with session.client("s3") as s3_client:
        obj = await s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        body = await obj["Body"].read()
        return BytesIO(body)
