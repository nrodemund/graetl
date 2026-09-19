"""The runner <-> server event protocol.

The runner writes one JSON object per line to stdout, prefixed with
``@@GRAETL@@``. Anything the pipeline prints normally (or any library writing
to stdout/stderr) arrives without the prefix and is turned into a plain console
line by the supervisor - so a noisy third-party library can never corrupt the
protocol.
"""

from __future__ import annotations

import io
import sys
import threading
from typing import Any, TextIO

from graetl.utils import dumps, loads, now_iso

EVENT_PREFIX = "@@GRAETL@@"


class EventWriter:
    """Thread-safe emitter of protocol events."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self.stream = stream or sys.__stdout__
        self._lock = threading.Lock()

    def emit(self, kind: str, payload: dict[str, Any] | None = None) -> None:
        event = {"kind": kind, "ts": now_iso(), **(payload or {})}
        line = EVENT_PREFIX + dumps(event)
        with self._lock:
            try:
                self.stream.write(line + "\n")
                self.stream.flush()
            except (ValueError, OSError):  # pragma: no cover - stream closed during shutdown
                pass

    def log(self, message: str, level: str = "info", **extra: Any) -> None:
        self.emit("log", {"level": level, "message": message, **extra})


def parse_line(line: str) -> dict[str, Any] | None:
    """Parse one line coming from a runner process, or ``None`` if it is raw output."""
    line = line.rstrip("\r\n")
    if not line:
        return None
    if line.startswith(EVENT_PREFIX):
        event = loads(line[len(EVENT_PREFIX) :])
        if isinstance(event, dict):
            return event
        return None
    return {
        "kind": "log",
        "ts": now_iso(),
        "level": "info",
        "message": line,
        "source": "stdout",
    }


class StreamCapture(io.TextIOBase):
    """Replacement for ``sys.stdout`` / ``sys.stderr`` inside the runner.

    Everything pipeline code prints becomes a structured console line, tagged
    with the step that is currently executing.
    """

    def __init__(self, writer: EventWriter, level: str, source: str) -> None:
        super().__init__()
        self._writer = writer
        self._level = level
        self._source = source
        self._buffer = ""
        self._lock = threading.Lock()
        self.context_step: str | None = None

    def write(self, text: str) -> int:  # type: ignore[override]
        if not text:
            return 0
        with self._lock:
            self._buffer += text
            while "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                if line.strip():
                    self._writer.emit(
                        "log",
                        {
                            "level": self._level,
                            "message": line.rstrip(),
                            "source": self._source,
                            "step": self.context_step,
                        },
                    )
        return len(text)

    def flush(self) -> None:  # type: ignore[override]
        with self._lock:
            if self._buffer.strip():
                self._writer.emit(
                    "log",
                    {
                        "level": self._level,
                        "message": self._buffer.rstrip(),
                        "source": self._source,
                        "step": self.context_step,
                    },
                )
            self._buffer = ""

    def isatty(self) -> bool:  # type: ignore[override]
        return False

    def writable(self) -> bool:  # type: ignore[override]
        return True
