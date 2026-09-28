"""Prove the service's file log sink rotates instead of growing unbounded.

``main()`` calls ``common.config.setup_logging(SERVICE_NAME, log_file=Path(...))`` exactly
once, with no override of `groovemap-runtime`'s handler construction, so the file handler it
installs on the root logger is whatever `setup_logging` builds for a given `log_file`. This
test drives the real (unmocked) `setup_logging` the same way `graphinator.main()` does and
asserts the resulting file handler is a size-capped `RotatingFileHandler` honoring
`LOG_FILE_MAX_BYTES` / `LOG_FILE_BACKUP_COUNT`, not an unbounded `FileHandler`.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_setup_logging_file_sink_is_size_capped_and_rotates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`setup_logging(..., log_file=...)`, called exactly as `main()` calls it, must install
    a `RotatingFileHandler` bounded by `LOG_FILE_MAX_BYTES` / `LOG_FILE_BACKUP_COUNT` rather
    than an unbounded `FileHandler` that would grow the `/logs` file forever.
    """
    from common.config import setup_logging

    max_bytes = 512
    backup_count = 2
    monkeypatch.setenv("LOG_FILE_MAX_BYTES", str(max_bytes))
    monkeypatch.setenv("LOG_FILE_BACKUP_COUNT", str(backup_count))
    log_file = tmp_path / "discogs-graph-enricher.log"

    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    try:
        setup_logging("discogs-graph-enricher", log_file=log_file)

        file_handlers = [h for h in root_logger.handlers if isinstance(h, RotatingFileHandler)]
        assert len(file_handlers) == 1, "expected exactly one RotatingFileHandler on the root logger"
        handler = file_handlers[0]
        assert handler.maxBytes == max_bytes
        assert handler.backupCount == backup_count
        assert not any(isinstance(h, logging.FileHandler) and not isinstance(h, RotatingFileHandler) for h in root_logger.handlers), (
            "an unbounded FileHandler would grow the /logs file without limit"
        )

        # Drive enough records through the real handler to force a rollover and prove the
        # bound is enforced end-to-end, not merely configured.
        plain_logger = logging.getLogger("test-discogs-graph-enricher-rotation")
        plain_logger.propagate = False
        plain_logger.handlers = [handler]
        plain_logger.setLevel(logging.INFO)
        try:
            for _ in range(100):
                plain_logger.info("x" * 100)
            handler.flush()
        finally:
            plain_logger.handlers.clear()

        rotated_files = sorted(tmp_path.glob(f"{log_file.name}*"))
        assert len(rotated_files) > 1, "log did not roll over under LOG_FILE_MAX_BYTES pressure"
        assert len(rotated_files) <= backup_count + 1
        assert all(path.stat().st_size <= max_bytes for path in rotated_files)
    finally:
        for h in root_logger.handlers:
            if h not in original_handlers:
                h.close()
        root_logger.handlers = original_handlers
        root_logger.setLevel(original_level)
