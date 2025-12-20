import subprocess
from pathlib import Path
from fastapi import FastAPI, HTTPException

app = FastAPI()

VENV_PYTHON = r"C:\Project\Python\Cocoonы\lgbm-env\Scripts\python.exe"

SCRIPT_PATH = str(Path(__file__).with_name("lgbm_infer.py"))

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/run-lgbm-infer")
def run_lgbm_infer():
    try:
        p = subprocess.run(
            [VENV_PYTHON, SCRIPT_PATH],
            capture_output=True,
            text=True,
            check=True,
        )
        return {"status": "ok", "stdout": p.stdout[-4000:]}
    except subprocess.CalledProcessError as e:
        raise HTTPException(
            status_code=500,
            detail={"status": "failed", "stdout": e.stdout[-4000:], "stderr": e.stderr[-4000:]},
        )