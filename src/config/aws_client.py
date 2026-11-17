import boto3
from functools import lru_cache
from botocore.config import Config
from src.config.settings import get_settings


def _aws_credentials(settings) -> dict:
    credentials = {
        "aws_access_key_id": settings.aws_access_key_id,
        "aws_secret_access_key": settings.aws_secret_access_key,
    }
    if settings.aws_session_token:
        credentials["aws_session_token"] = settings.aws_session_token
    return credentials


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def configure_duckdb_s3(con) -> None:
    """Configure DuckDB's httpfs extension with local or Lambda credentials."""
    settings = get_settings()
    # Lambda does not expose a writable home directory; /tmp is its writable
    # runtime volume and lets DuckDB cache/install the httpfs extension.
    con.execute("SET home_directory='/tmp';")
    con.execute("SET TimeZone='UTC';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{_sql_literal(settings.aws_region)}';")
    con.execute(f"SET s3_access_key_id='{_sql_literal(settings.aws_access_key_id)}';")
    con.execute(f"SET s3_secret_access_key='{_sql_literal(settings.aws_secret_access_key)}';")
    if settings.aws_session_token:
        con.execute(f"SET s3_session_token='{_sql_literal(settings.aws_session_token)}';")


@lru_cache(maxsize=1)
def get_s3_client():
    settings = get_settings()
    return boto3.client(
        "s3",
        region_name=settings.aws_region,
        **_aws_credentials(settings),
        config=Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            max_pool_connections=10,
        ),
    )


@lru_cache(maxsize=1)
def get_events_client():
    settings = get_settings()
    return boto3.client(
        "events",
        region_name=settings.aws_region,
        **_aws_credentials(settings),
    )
