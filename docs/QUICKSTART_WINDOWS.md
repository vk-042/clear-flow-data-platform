# Windows quickstart

Install Python 3.11 and VS Code. Open the extracted `clearflow` folder in VS Code,
then open a PowerShell terminal. Activation is optional with these commands:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m clearflow.cli demo --count 100
.\.venv\Scripts\python.exe -m streamlit run src/clearflow/dashboard.py
```

Open http://localhost:8501. Switch Banking/Healthcare. Inspect state, review flags,
quality outcomes, and reconciliation. Ctrl+C stops the dashboard.

## Streaming mode

Install and start Docker Desktop with WSL 2. Start with an 8 GB RAM allocation.
Stop local Streamlit first if it uses port 8501.

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up -d --build processor dashboard
docker compose --profile live up -d producer
docker compose logs -f processor
```

First startup downloads images and connectors. Stop without deleting data:

```powershell
docker compose stop producer
docker compose --profile live --profile orchestration down
```

Use `down -v` only when you intend to erase generated demo volumes.

## Learning order

1. `contracts.py`: fields, types, decimals, validation.
2. `generator.py`: lifecycles and deliberately bad input.
3. `storage.py`: keys, tables, transactions.
4. `processor.py`: deduplication and reconstruction.
5. `reconcile.py`: correctness against an independent-style reference.
6. `streaming.py`: Kafka, Iceberg, checkpoints.
7. `anomaly.py`, `dashboard.py`: scoring and presentation.
8. Tests/runbook: failure, replay, recovery.
