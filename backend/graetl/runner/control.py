"""Cooperative pause/stop for a running pipeline process.

The server never kills a run to pause it. It writes a control word into the
``control`` column of the run's row in the project's target database; this
watcher picks it up on its own connection and flips a
``threading.Event``. The executor checks that flag at safe points (between
entities, between modules, and wherever pipeline code calls
``ctx.checkpoint()``), so state is always consistent.

A hard kill remains available as a last resort (stop grace period elapsed).
"""

from __future__ import annotations

import threading
from typing import Callable

from graetl.store.db import Target, connect


class ControlWatcher:
    """Polls the control column of one run."""

    def __init__(
        self,
        target: Target,
        run_id: int,
        *,
        poll_seconds: float = 0.5,
        on_change: Callable[[str], None] | None = None,
    ) -> None:
        self.target = target
        self.run_id = run_id
        self.poll_seconds = poll_seconds
        self.on_change = on_change

        self._resume = threading.Event()
        self._resume.set()
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_seq = -1
        self.stop_requested = False
        self.paused = False

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        self._thread = threading.Thread(target=self._loop, name="graetl-control", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._shutdown.set()
        self._resume.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def __enter__(self) -> ControlWatcher:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------------------------------------------------------------- polling

    def _loop(self) -> None:
        db = None
        try:
            db = connect(self.target)
        except Exception:  # pragma: no cover - db not reachable; run uncontrolled
            return
        try:
            while not self._shutdown.wait(self.poll_seconds):
                try:
                    row = db.fetchone(
                        "SELECT control, control_seq FROM [[runs]] WHERE id = ?", (self.run_id,)
                    )
                except Exception:  # pragma: no cover
                    continue
                if row is None:
                    continue
                seq = int(row["control_seq"])
                if seq == self._last_seq:
                    continue
                self._last_seq = seq
                self.apply(row["control"])
        finally:
            if db is not None:
                db.close()

    def apply(self, control: str | None) -> None:
        if control == "pause":
            if not self.paused:
                self.paused = True
                self._resume.clear()
                self._notify("pause")
        elif control == "resume":
            if self.paused:
                self.paused = False
                self._resume.set()
                self._notify("resume")
        elif control == "stop":
            self.stop_requested = True
            self.paused = False
            self._resume.set()
            self._notify("stop")

    def _notify(self, what: str) -> None:
        if self.on_change:
            try:
                self.on_change(what)
            except Exception:  # pragma: no cover
                pass

    # ------------------------------------------------------------- executor API

    def wait_if_paused(self) -> None:
        """Block while paused (returns immediately when a stop arrives)."""
        while not self._resume.wait(timeout=0.25):
            if self.stop_requested or self._shutdown.is_set():
                return
