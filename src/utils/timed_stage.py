import time
from contextlib import contextmanager
import logging

@contextmanager
def timed_stage(stage_name: str, logger: logging.Logger):
    start_time = time.time()
    logger.info(f"{stage_name} started.")
    try:
        yield
        elapsed = time.time() - start_time
        logger.info(
            f"{stage_name} completed | duration={elapsed:.2f}s"
        )
    except Exception:
        elapsed = time.time() - start_time
        logger.error(
            f"{stage_name} failed | duration={elapsed:.2f}s"
        )
        raise