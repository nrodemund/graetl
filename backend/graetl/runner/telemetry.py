"""Process telemetry for a running pipeline.

Uses ``psutil`` when it is installed and falls back to the standard library, so
the resource panel works on a bare install too.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from typing import Any

try:  # optional, gives CPU%, thread and handle counts on every platform
    import psutil  # type: ignore

    _PROCESS = psutil.Process()
    _PROCESS.cpu_percent(interval=None)  # prime the counter
except Exception:  # pragma: no cover - psutil is optional
    psutil = None  # type: ignore
    _PROCESS = None


def _rss_bytes() -> int | None:
    """Resident memory, without psutil."""
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/self/statm", encoding="ascii") as fh:
                pages = int(fh.read().split()[1])
            return pages * os.sysconf("SC_PAGE_SIZE")
        except Exception:  # pragma: no cover
            return None
    if sys.platform == "win32":  # pragma: no cover - Windows only
        try:
            import ctypes
            from ctypes import wintypes

            class _COUNTERS(ctypes.Structure):
                _fields_ = [
                    ("cb", wintypes.DWORD),
                    ("PageFaultCount", wintypes.DWORD),
                    ("PeakWorkingSetSize", ctypes.c_size_t),
                    ("WorkingSetSize", ctypes.c_size_t),
                    ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                    ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                    ("PagefileUsage", ctypes.c_size_t),
                    ("PeakPagefileUsage", ctypes.c_size_t),
                ]

            counters = _COUNTERS()
            counters.cb = ctypes.sizeof(_COUNTERS)
            handle = ctypes.windll.kernel32.GetCurrentProcess()
            if ctypes.windll.psapi.GetProcessMemoryInfo(
                handle, ctypes.byref(counters), counters.cb
            ):
                return int(counters.WorkingSetSize)
        except Exception:
            return None
        return None
    try:  # macOS and the rest: peak RSS is the best the stdlib offers
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(peak if sys.platform == "darwin" else peak * 1024)
    except Exception:  # pragma: no cover
        return None


class ProcessSampler:
    """Tracks CPU and memory of this process between calls to :meth:`sample`."""

    def __init__(self) -> None:
        self._wall = time.monotonic()
        self._cpu = time.process_time()
        self._peak_rss = 0

    def sample(self) -> dict[str, Any]:
        now_wall = time.monotonic()
        now_cpu = time.process_time()
        wall_delta = max(now_wall - self._wall, 1e-6)

        out: dict[str, Any] = {
            "pid": os.getpid(),
            "threads": threading.active_count(),
            "backend": "psutil" if _PROCESS is not None else "stdlib",
        }
        rss = None
        if _PROCESS is not None:  # pragma: no cover - depends on the install
            try:
                with _PROCESS.oneshot():
                    rss = _PROCESS.memory_info().rss
                    out["cpu_percent"] = round(_PROCESS.cpu_percent(interval=None), 1)
                    out["threads"] = _PROCESS.num_threads()
                    try:
                        out["open_files"] = len(_PROCESS.open_files())
                    except Exception:
                        pass
            except Exception:
                rss = None
        if rss is None:
            rss = _rss_bytes()
            out["cpu_percent"] = round((now_cpu - self._cpu) / wall_delta * 100, 1)
        if rss:
            out["rss_mb"] = round(rss / 1048576, 1)
            self._peak_rss = max(self._peak_rss, rss)
            out["peak_rss_mb"] = round(self._peak_rss / 1048576, 1)

        self._wall, self._cpu = now_wall, now_cpu
        return out
