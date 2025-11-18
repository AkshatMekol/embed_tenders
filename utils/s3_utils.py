import boto3
from io import BytesIO
from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET

_s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

def list_s3_pdfs(prefix: str):
    response = _s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    return [
        obj["Key"] 
        for obj in response.get("Contents", []) 
        if obj["Key"].lower().endswith(".pdf")
    ]

def fetch_pdf(key: str) -> BytesIO:
    obj = _s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    return BytesIO(obj["Body"].read())
