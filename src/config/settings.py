from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    # AWS
    aws_access_key_id: str = Field(..., env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = Field(..., env="AWS_SECRET_ACCESS_KEY")
    aws_session_token: str | None = Field(default=None, env="AWS_SESSION_TOKEN")
    aws_region: str = Field(default="ap-southeast-1", env="AWS_REGION")

    # S3 bucket
    s3_bucket: str = Field(default="finpulse-lake", env="S3_BUCKET")

    # S3 prefixes
    s3_prefix_raw: str = "raw/"
    s3_prefix_bronze: str = "bronze/"
    s3_prefix_silver: str = "silver/"
    s3_prefix_gold: str = "gold/"
    s3_prefix_quarantine: str = "quarantine/"
    s3_prefix_state: str = "state/"
    s3_state_key: str = "state/state.json"

    # Ingestion
    batch_size: int = Field(default=50, env="BATCH_SIZE")
    raw_transactions_key: str = "raw/transactions.parquet"
    raw_users_key: str = "raw/users.parquet"
    raw_cards_key: str = "raw/cards.parquet"

    # EventBridge
    eventbridge_rule_name: str = Field(
        default="finpulse-ingestion-trigger", env="EVENTBRIDGE_RULE_NAME"
    )

    # DQ thresholds — Tier 2 Business Logic
    cur_high_risk_threshold: float = 0.85          # CUR > 85% → HIGH_RISK (CFPB 2023)
    fico_subprime_cutoff: int = 620                # FICO < 620 → Subprime (Basel II)
    velocity_max_txn_per_window: int = 3           # More than this per card in a micro-batch

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
