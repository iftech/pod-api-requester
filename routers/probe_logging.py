"""Keep successful readiness checks out of Uvicorn's access logs."""

import logging

PROBE_PATHS = frozenset({"/healthz", "/openapi.json"})


class ProbeAccessFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Uvicorn supplies client, method, path, HTTP version, and status separately.
        # Keep unfamiliar records rather than relying on formatted-message matching.
        if not isinstance(record.args, tuple) or len(record.args) != 5:
            return True
        _, method, path, _, status = record.args
        return not (
            record.levelno < logging.WARNING
            and method in ("GET", "HEAD")
            and isinstance(path, str)
            and path.partition("?")[0] in PROBE_PATHS
            and isinstance(status, int)
            and 200 <= status < 300
        )


def suppress_successful_probe_logs() -> None:
    logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(item, ProbeAccessFilter) for item in logger.filters):
        logger.addFilter(ProbeAccessFilter())
