import boto3
from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET
from io import BytesIO

# ================= SYNCHRONOUS S3 CLIENT =================
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# ================= LIST PDF FILES =================
def list_pdfs_sync(prefix):
    """Return a list of PDF keys under the given S3 prefix."""
    pdf_keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".pdf"):
                pdf_keys.append(key)
    return pdf_keys

# ================= DOWNLOAD PDF =================
def download_pdf_sync(key):
    """Download PDF from S3 and return as BytesIO."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return BytesIO(obj['Body'].read())

