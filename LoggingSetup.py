import json, logging, logging.config, os, time, uuid
from contextlib import contextmanager

run_id = str(uuid.uuid4())
project_id = os.environ["QONIC_PROJECT_ID"]
model_id = os.environ["QONIC_MODEL_ID"]

class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for machine-readable logs."""
    def format(self, record: logging.LogRecord) -> str:
        base = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        # Include any 'extra' fields added via LoggerAdapter (e.g., run_id, projectId, modelId)
        for key, value in record.__dict__.items():
            if key not in ("args", "msg", "exc_info", "exc_text", "stack_info") and not key.startswith("_"):
                base.setdefault(key, value)
        if record.exc_info:
            base["exc"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)

def setup_logging(log_dir="logs", base_name="sync.log", level=logging.INFO):
    os.makedirs(log_dir, exist_ok=True)
    logfile_path = os.path.join(log_dir, base_name)
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "console": {
                "format": "%(asctime)s %(levelname)s %(name)s [run=%(run_id)s proj=%(projectId)s model=%(modelId)s] %(message)s"
            },
            "json": {
                "()": JsonFormatter
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "console",
                "level": level,
            },
            "file": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "filename": logfile_path,
                "when": "midnight",      # rotate daily
                "backupCount": 14,       # keep two weeks
                "encoding": "utf-8",
                "formatter": "json",
                "level": level,
            },
        },
        "root": {
            "handlers": ["console", "file"],
            "level": level,
        },
    })

@contextmanager
def log_span(logger, name: str, **fields):
    start = time.monotonic()
    logger.info("START " + name, extra=fields)
    try:
        yield
    except Exception:
        logger.exception("ERROR " + name, extra=fields)
        raise
    finally:
        dur = round(time.monotonic() - start, 3)
        logger.info("END " + name, extra={**fields, "duration_s": dur})

def get_logger():
    base = logging.getLogger("qonic.maximo.sync")
    # Attach default contextual fields to every log entry
    return logging.LoggerAdapter(base, {"run_id": run_id, "projectId": project_id, "modelId": model_id})
