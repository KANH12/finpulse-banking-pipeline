"""Run Producer, DQ Gate, and Analytics sequentially from one Lambda invocation."""
from src.analytics.handler import handler as analytics_handler
from src.pipeline.handler import handler as pipeline_handler
from src.producer.handler import handler as producer_handler


def handler(event: dict, context) -> dict:
    producer_result = producer_handler(event or {}, context)
    bronze_key = producer_result.get("bronze_key")
    if not bronze_key:
        return {"status": producer_result.get("status", "EXHAUSTED"), "producer": producer_result}

    pipeline_result = pipeline_handler({"bronze_key": bronze_key}, context)
    analytics_result = analytics_handler({}, context)
    return {
        "status": "OK",
        "producer": producer_result,
        "pipeline": pipeline_result,
        "analytics": analytics_result,
    }
