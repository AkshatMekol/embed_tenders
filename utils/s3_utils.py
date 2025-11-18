import boto3
from io import BytesIO
from .config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


def list_s3_pdfs(bucket, prefix):
    s3 = get_s3_client()
    objs = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for o in objs.get("Contents", []) if o["Key"].lower().endswith(".pdf")]


def fetch_pdf(bucket, key):
    s3 = get_s3_client()
    obj = s3.get_object(Bucket=bucket, Key=key)
    return BytesIO(obj["Body"].read())

