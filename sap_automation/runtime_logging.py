from __future__ import annotations

import logging
import sys
from pathlib import Path


def configure_run_logger(*, output_root: Path, run_id: str) -> tuple[logging.Logger, Path]:
    log_dir = output_root.expanduser().resolve() / "runs" / run_id / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "sap_session.log"

    logger = logging.getLogger(f"sap_automation.run.{run_id}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger, log_path
