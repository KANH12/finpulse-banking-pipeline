"""Shared test configuration; no test talks to real AWS services."""
import os

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test-access-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test-secret-key")
os.environ.setdefault("AWS_REGION", "ap-southeast-2")
os.environ.setdefault("S3_BUCKET", "finpulse-test")
os.environ.setdefault("BATCH_SIZE", "10")
