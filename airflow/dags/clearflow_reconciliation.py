"""Enable only after placing a complete, immutable reference snapshot at REFERENCE_PATH."""
import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(dag_id="clearflow_reconciliation", start_date=datetime(2025, 1, 1),
     schedule="@daily", catchup=False, max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
     tags=["clearflow", "banking", "healthcare"])
def reconciliation_pipeline():
    @task
    def compare_snapshot():
        from clearflow.storage import get_engine
        from clearflow.reconcile import reconcile
        # Airflow 2 SQLAlchemy 1.4 uses psycopg2, while the application uses psycopg3.
        url = os.environ["DATABASE_URL"].replace("postgresql+psycopg:", "postgresql+psycopg2:")
        report = reconcile(get_engine(url), os.environ["REFERENCE_PATH"])
        if report["issues"]:
            raise ValueError(f"Reconciliation failed: {len(report['issues'])} issues; inspect persisted report")
        return {"run_id": report["run_id"], "matched": report["matched"]}
    compare_snapshot()

reconciliation_pipeline()
