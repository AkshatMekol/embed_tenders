import boto3
from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET
from io import BytesIO

def list_s3_pdfs(bucket, prefix):
    s3 = boto3.client("s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    pdfs = []

    for obj in objs.get("Contents", []):
        key = obj["Key"]
        if key.lower().endswith(".pdf"):
            pdfs.append(key)

    print(f"PDFs found: {len(pdfs)}")
    return pdfs

def download_pdf(key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return BytesIO(obj['Body'].read())

