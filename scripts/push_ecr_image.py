"""Build the Lambda image and push it to the project's ECR repository.

Run from the project root in the user's PowerShell session, which has access
to Docker Desktop:
    .\.venv\Scripts\python.exe scripts\push_ecr_image.py
"""
import base64
import subprocess
import sys
from pathlib import Path

import boto3

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import get_settings


def main() -> None:
    settings = get_settings()
    sts = boto3.client(
        "sts",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    account_id = sts.get_caller_identity()["Account"]
    repository = f"{account_id}.dkr.ecr.{settings.aws_region}.amazonaws.com/finpulse-microbatch-pipeline"
    registry = repository.split("/", maxsplit=1)[0]

    ecr = boto3.client(
        "ecr",
        region_name=settings.aws_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )
    token = ecr.get_authorization_token()["authorizationData"][0]["authorizationToken"]
    password = base64.b64decode(token).decode("utf-8").split(":", maxsplit=1)[1]

    subprocess.run(
        ["docker", "login", "--username", "AWS", "--password-stdin", registry],
        input=password,
        text=True,
        check=True,
    )
    subprocess.run(
        [
            "docker", "buildx", "build",
            "--platform", "linux/amd64",
            "--provenance=false",
            "--sbom=false",
            "--load",
            "-t", "finpulse-microbatch-pipeline:latest",
            ".",
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )
    subprocess.run(["docker", "tag", "finpulse-microbatch-pipeline:latest", f"{repository}:latest"], check=True)
    subprocess.run(["docker", "push", f"{repository}:latest"], check=True)
    print(f"Pushed image: {repository}:latest")


if __name__ == "__main__":
    main()
