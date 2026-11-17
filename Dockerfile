FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ./
RUN pip install --no-cache-dir --only-binary=:all: -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

COPY src/ "${LAMBDA_TASK_ROOT}/src/"

CMD ["src.orchestrator.handler.handler"]
