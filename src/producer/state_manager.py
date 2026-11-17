import json
from datetime import datetime, timezone
from src.config.settings import get_settings
from src.config.aws_client import get_s3_client
from src.config.logger import get_logger

log = get_logger("finpulse.state_manager")


def load_state() -> dict:
    settings = get_settings()
    client = get_s3_client()
    try:
        obj = client.get_object(Bucket=settings.s3_bucket, Key=settings.s3_state_key)
        state = json.loads(obj["Body"].read())
        log.info("State loaded", extra={"last_timestamp": state.get("last_timestamp")})
        return state
    except client.exceptions.NoSuchKey:
        log.info("No state found, initializing fresh state")
        return {"last_timestamp": None, "total_processed": 0, "last_run": None}


def save_state(last_timestamp: str, total_processed: int) -> None:
    settings = get_settings()
    client = get_s3_client()
    state = {
        "last_timestamp": last_timestamp,
        "total_processed": total_processed,
        "last_run": datetime.now(timezone.utc).isoformat(),
    }
    client.put_object(
        Bucket=settings.s3_bucket,
        Key=settings.s3_state_key,
        Body=json.dumps(state, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    log.info("State saved", extra={"last_timestamp": last_timestamp, "total_processed": total_processed})
