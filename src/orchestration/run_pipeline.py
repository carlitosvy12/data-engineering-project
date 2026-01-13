import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime


# Base project path
BASE_PATH = Path(__file__).resolve().parents[2]

# Logs folder 
LOGS_PATH = BASE_PATH / "logs"
LOGS_PATH.mkdir(exist_ok=True)

# Logging configuration
logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(levelname)s - %(message)s",handlers=[
        logging.FileHandler(LOGS_PATH / "pipeline.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

def write_alert_file(message: str):
    # Create an alert file if the pipeline fails
    alert_path = LOGS_PATH / "ALERT_PIPELINE_FAILED.txt"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    alert_path.write_text(
        f"[{ts}] PIPELINE FAILED\n{message}\n",
        encoding="utf-8"
    )


def write_success_file():
    # Create a success file when the pipeline finishes correctly
    success_path = LOGS_PATH / "PIPELINE_SUCCESS.txt"
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success_path.write_text(
        f"[{ts}] PIPELINE FINISHED SUCCESSFULLY\n",
        encoding="utf-8"
    )

def run_step(step_name: str, module: str):
    # Run one pipeline step as a Python module
    logging.info(f"=== START STEP: {step_name} ===")


    result = subprocess.run(
        [sys.executable, "-m", module],
        capture_output=True,
        text=True
    )

    # Log standard output if present
    if result.stdout:
        logging.info(result.stdout.strip())


    # Log errors or warnings from stderr
    if result.stderr:
        logging.warning(result.stderr.strip())

    # Stop the pipeline if the step fails
    if result.returncode != 0:
        logging.error(f"STEP FAILED: {step_name}")
        raise RuntimeError(f"Pipeline failed at step: {step_name}")

    logging.info(f"=== END STEP: {step_name} ===")


if __name__ == "__main__":
    try:
        # Execute all pipeline steps in order
        run_step("Data Quality Checks (RAW)", "src.validation.run_quality_checks")
        run_step("Clean to Staging", "src.transformation.clean_to_staging")
        run_step("Integrate Datasets", "src.transformation.integrate_data")
        run_step("Load to Data Warehouse (SQLite)", "src.warehouse.load_to_dw_sqlite")
        run_step("Publish to Azure Blob (Cloud)", "src.serving.publish_to_azure_blob")

        # Mark pipeline as successful
        logging.info("PIPELINE FINISHED SUCCESSFULLY")
        write_success_file()


    except Exception as e:
        # Handle any failure and create alert file
        logging.exception("PIPELINE FAILED")
        write_alert_file(str(e))
        raise
