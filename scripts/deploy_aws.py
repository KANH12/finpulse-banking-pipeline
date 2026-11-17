"""Deploy FinPulse as an EventBridge Scheduler-triggered Lambda container."""
import json
import os
import time

import boto3
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "ap-southeast-2")
BUCKET = os.getenv("S3_BUCKET", "finpulse-lake")
FUNCTION_NAME = "finpulse-microbatch-pipeline"
SCHEDULE_NAME = "finpulse-pipeline-every-5-min"
LAMBDA_ROLE = "finpulse-microbatch-lambda-role"
SCHEDULER_ROLE = "finpulse-microbatch-scheduler-role"
IMAGE_URI = os.environ["FINPULSE_IMAGE_URI"]

iam = boto3.client("iam")
lambda_client = boto3.client("lambda", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)


def ensure_role(name: str, service: str, policy: dict) -> str:
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}],
    }
    try:
        role = iam.get_role(RoleName=name)["Role"]
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]
        time.sleep(10)
    iam.put_role_policy(RoleName=name, PolicyName=f"{name}-policy", PolicyDocument=json.dumps(policy))
    return role["Arn"]


lambda_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": f"arn:aws:s3:::{BUCKET}"},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": f"arn:aws:s3:::{BUCKET}/*"},
        {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
    ],
}

lambda_role_arn = ensure_role(LAMBDA_ROLE, "lambda.amazonaws.com", lambda_policy)
environment = {
    "Variables": {
        "S3_BUCKET": BUCKET,
        "BATCH_SIZE": "10",
        "SCHEDULER_MANAGED": "true",
    }
}

try:
    function_arn = lambda_client.get_function(FunctionName=FUNCTION_NAME)["Configuration"]["FunctionArn"]
    lambda_client.update_function_code(FunctionName=FUNCTION_NAME, ImageUri=IMAGE_URI, Publish=True)
    lambda_client.get_waiter("function_updated").wait(FunctionName=FUNCTION_NAME)
    lambda_client.update_function_configuration(
        FunctionName=FUNCTION_NAME,
        Role=lambda_role_arn,
        Timeout=300,
        MemorySize=2048,
        Environment=environment,
    )
except lambda_client.exceptions.ResourceNotFoundException:
    function_arn = lambda_client.create_function(
        FunctionName=FUNCTION_NAME,
        Role=lambda_role_arn,
        PackageType="Image",
        Code={"ImageUri": IMAGE_URI},
        Timeout=300,
        MemorySize=2048,
        Environment=environment,
        Architectures=["x86_64"],
    )["FunctionArn"]

lambda_client.get_waiter("function_active").wait(FunctionName=FUNCTION_NAME)

scheduler_policy = {
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": function_arn}],
}
scheduler_role_arn = ensure_role(SCHEDULER_ROLE, "scheduler.amazonaws.com", scheduler_policy)
schedule = {
    "Name": SCHEDULE_NAME,
    "ScheduleExpression": "rate(5 minutes)",
    "FlexibleTimeWindow": {"Mode": "OFF"},
    "State": "ENABLED",
    "Target": {
        "Arn": function_arn,
        "RoleArn": scheduler_role_arn,
        "Input": json.dumps({"batch_size_override": 10}),
    },
}
try:
    scheduler.create_schedule(**schedule)
except scheduler.exceptions.ConflictException:
    scheduler.update_schedule(**schedule)

print(json.dumps({"function": FUNCTION_NAME, "schedule": SCHEDULE_NAME, "region": REGION, "batch_size": 10}, indent=2))
