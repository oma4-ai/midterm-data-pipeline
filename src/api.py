from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"

for path in (str(PROJECT_ROOT), str(SRC_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

from bootstrap import ensure_project_root

ensure_project_root()

from fastapi import FastAPI, HTTPException
from src.file_router import route_file
from config.settings import INPUT_FILE

app = FastAPI(
    title="Midterm Data Pipeline API",
    description="API for the Hybrid Data Pipeline",
    version="1.0.0",
)


@app.get("/")
def root():
    return {
        "project": "midterm-data-pipeline",
        "status": "running",
    }


@app.get("/health")
def health():
    return {
        "status": "healthy",
    }


@app.get("/pipeline/inspect")
def inspect_pipeline(file: str | None = None):
    target = file or str(INPUT_FILE)

    try:
        decision = route_file(target)
        return {
            "status": "success",
            "message": "File inspected successfully",
            "decision": decision,
        }
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

from src.batch_loader import load_csv_to_raw as load_with_python
from src.spark_loader import load_csv_to_raw as load_with_spark


@app.post("/pipeline/run")
def run_pipeline(file: str | None = None):
    target = file or str(INPUT_FILE)

    try:
        decision = route_file(target)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    run_id = decision["run_id"]
    engine = decision["engine"]

    try:
        if engine == "python_batch":
            stats = load_with_python(
                decision["file_path"],
                run_id,
                engine_used=engine,
            )
        else:
            stats = load_with_spark(
                decision["file_path"],
                run_id,
                engine_used=engine,
                close_spark=True,
            )

        return {
            "status": "success",
            "run_id": run_id,
            "file_name": decision["file_name"],
            "engine": engine,
            "decision": decision,
            "stats": stats,
        }

    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Pipeline execution failed: {type(exc).__name__}: {exc}",
        )
