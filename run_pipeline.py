"""FinPulse local pipeline runner.

Runs one complete cycle: Producer -> Data Quality Gate -> Analytics.
"""
from src.config.logger import get_logger
from src.producer.handler import handler as producer_handler
from src.pipeline.handler import handler as pipeline_handler
from src.analytics.handler import handler as analytics_handler

log = get_logger("finpulse.runner")

def main():
    print("\n=======================================================")
    print(">>> STARTING FINPULSE PIPELINE CYCLE")
    print("=======================================================")

    # Step 1: Run the producer (Lambda 1).
    print("\n[STEP 1] Producer: read micro-batch -> write Bronze...")
    res_producer = producer_handler({}, None)
    print(f"-> Producer result: {res_producer}")

    bronze_key = res_producer.get("bronze_key")
    if not bronze_key:
        print("-> No new batch available (dataset exhausted). Stopping.")
        return

    # Step 2: Run the Data Quality Gate (Lambda 2).
    print(f"\n[STEP 2] DQ Gate: validate {bronze_key} -> route to Silver / Quarantine...")
    event_lambda2 = {"bronze_key": bronze_key}
    res_pipeline = pipeline_handler(event_lambda2, None)
    print(f"-> DQ Gate result: {res_pipeline}")

    # Step 3: Run analytics (Lambda 3).
    print("\n[STEP 3] Analytics: materialise Gold fact and dimensions...")
    res_analytics = analytics_handler({}, None)
    print(f"-> Analytics Gold result: {res_analytics}")

    print("\n=======================================================")
    print(">>> PIPELINE CYCLE COMPLETED SUCCESSFULLY!")
    print("Open the dashboard with: streamlit run src/dashboard/app.py")
    print("=======================================================\n")

if __name__ == "__main__":
    main()
