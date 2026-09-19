"""A result cache for pipeline functions.

The case this exists for: a lookup that is the same every time and expensive
once. ``GetUnit(123)`` hits the database to learn that 123 means grams, and then
does it again for the next 26,999 entities; ``LoadParameterDict()`` builds the
same dictionary from the same file over and over. Marking the function cached
turns both into one call.

Design notes, because a cache that is wrong is worse than no cache:

* **Process-local.** Every run is its own OS process, so a cache never outlives
  a run and can never be stale across runs. Inside a run it is shared by every
  module, which is strictly better than the "at least per module" the feature
  needs.
* **``ctx`` is not part of the key.** It is the run context, one per worker;
  keying on it would defeat the cache and pin the context in memory. A cached
  function that reads different data for the same arguments *because of* ``ctx``
  is not a function, and should not be cached.
* **Unhashable arguments do not raise.** They fall through and call the real
  function, every time. A cache is an optimisation; it must never be the reason
  a pipeline fails. The same goes for an argument whose key would be enormous.
* **Types are part of the key**, so ``get(1)`` and ``get(True)`` and ``get("1")``
  are three different calls - Python would otherwise treat the first two as one.
* **Exceptions are not cached.** A failure is retried next time.

Thread-safe: the lock is held only around the dictionary, never around the call,
so two workers may compute the same value at once. That is the same trade
``functools.lru_cache`` makes, and it is the right one - holding a lock across
user code invites deadlock.
"""

from __future__ import annotations

import functools
import inspect
import threading
from collections import OrderedDict
from typing import Any, Callable, Iterator

#: Entries per cached function. Big enough that a lookup table of units, codes
#: or parameters fits whole; small enough that a runaway key space cannot eat
#: the process.
DEFAULT_MAXSIZE = 4096

#: Give up building a key past this nesting depth or this many elements. A key
#: that large costs more to build than the call it would save.
_MAX_DEPTH = 6
_MAX_ELEMENTS = 2048


class _Unhashable:
    """Sentinel: this argument cannot be part of a cache key."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging only
        return "<unhashable>"


UNHASHABLE = _Unhashable()

_default_maxsize = DEFAULT_MAXSIZE
_caches: list[FunctionCache] = []
_registry_lock = threading.Lock()


def set_default_maxsize(size: int | None) -> None:
    """Set the process-wide default, from configuration. ``None`` restores it."""
    global _default_maxsize
    _default_maxsize = int(size) if size else DEFAULT_MAXSIZE


def freeze(value: Any, depth: int = 0, budget: list[int] | None = None) -> Any:
    """A hashable stand-in for ``value``, or :data:`UNHASHABLE`.

    Containers are frozen recursively so a dictionary argument still caches;
    a dict becomes an order-insensitive frozenset of pairs, which is what
    equality on dicts means anyway.
    """
    if budget is None:
        budget = [_MAX_ELEMENTS]
    budget[0] -= 1
    if budget[0] < 0 or depth > _MAX_DEPTH:
        return UNHASHABLE

    if value is None or isinstance(value, (bool, int, float, complex, str, bytes)):
        # Type included: 1, 1.0 and True are one key to Python, three to a user.
        return (type(value).__name__, value)
    if isinstance(value, (list, tuple)):
        parts = tuple(freeze(item, depth + 1, budget) for item in value)
        return UNHASHABLE if any(p is UNHASHABLE for p in parts) else (type(value).__name__, parts)
    if isinstance(value, (set, frozenset)):
        parts = tuple(freeze(item, depth + 1, budget) for item in value)
        return UNHASHABLE if any(p is UNHASHABLE for p in parts) else ("set", frozenset(parts))
    if isinstance(value, dict):
        pairs = []
        for key, item in value.items():
            frozen_key = freeze(key, depth + 1, budget)
            frozen_item = freeze(item, depth + 1, budget)
            if frozen_key is UNHASHABLE or frozen_item is UNHASHABLE:
                return UNHASHABLE
            pairs.append((frozen_key, frozen_item))
        return ("dict", frozenset(pairs))
    try:
        hash(value)
    except Exception:  # noqa: BLE001 - anything at all means "do not cache"
        return UNHASHABLE
    return (type(value).__name__, value)


class FunctionCache:
    """The LRU behind one cached function."""

    __slots__ = ("name", "maxsize", "_entries", "_lock", "hits", "misses", "skipped", "evictions")

    def __init__(self, name: str, maxsize: int | None = None) -> None:
        self.name = name
        self.maxsize = maxsize
        self._entries: OrderedDict[Any, Any] = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.skipped = 0
        self.evictions = 0

    @property
    def limit(self) -> int:
        return self.maxsize if self.maxsize is not None else _default_maxsize

    def get(self, key: Any) -> tuple[bool, Any]:
        with self._lock:
            if key in self._entries:
                self._entries.move_to_end(key)
                self.hits += 1
                return True, self._entries[key]
            self.misses += 1
            return False, None

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            self._entries[key] = value
            self._entries.move_to_end(key)
            while len(self._entries) > self.limit:
                self._entries.popitem(last=False)
                self.evictions += 1

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return {
                "function": self.name,
                "entries": len(self._entries),
                "hits": self.hits,
                "misses": self.misses,
                "skipped": self.skipped,
                "evictions": self.evictions,
                "maxsize": self.limit,
            }


def cached(
    func: Callable[..., Any] | None = None,
    *,
    maxsize: int | None = None,
    name: str | None = None,
) -> Any:
    """Memoise a pipeline function, process-wide.

    ``maxsize`` overrides the default number of entries for this one function.
    The wrapper carries ``.cache`` (the :class:`FunctionCache`) so it can be
    inspected or cleared.
    """

    def decorate(target: Callable[..., Any]) -> Callable[..., Any]:
        cache = FunctionCache(name or getattr(target, "__name__", "function"), maxsize)
        skip_first = _takes_context(target)

        @functools.wraps(target)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = _key(args[1:] if skip_first and args else args, kwargs)
            if key is UNHASHABLE:
                cache.skipped += 1
                return target(*args, **kwargs)
            found, value = cache.get(key)
            if found:
                return value
            value = target(*args, **kwargs)
            cache.put(key, value)
            return value

        wrapper.cache = cache  # type: ignore[attr-defined]
        wrapper.graetl_cached = True  # type: ignore[attr-defined]
        with _registry_lock:
            _caches.append(cache)
        return wrapper

    return decorate(func) if callable(func) else decorate


def _key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    budget = [_MAX_ELEMENTS]
    frozen_args = tuple(freeze(a, 0, budget) for a in args)
    if any(a is UNHASHABLE for a in frozen_args):
        return UNHASHABLE
    if not kwargs:
        return frozen_args
    frozen_kwargs = []
    for key in sorted(kwargs):
        frozen = freeze(kwargs[key], 0, budget)
        if frozen is UNHASHABLE:
            return UNHASHABLE
        frozen_kwargs.append((key, frozen))
    return (frozen_args, tuple(frozen_kwargs))


def _takes_context(func: Callable[..., Any]) -> bool:
    """Is the first parameter the run context? Then it is not part of the key."""
    try:
        first = next(iter(inspect.signature(func).parameters))
    except (ValueError, TypeError, StopIteration):
        return False
    return first in ("ctx", "context")


def caches() -> Iterator[FunctionCache]:
    with _registry_lock:
        return iter(list(_caches))


def stats() -> list[dict[str, Any]]:
    """One row per cached function, for the run metrics."""
    return [cache.to_dict() for cache in caches()]


def clear_all() -> None:
    for cache in caches():
        cache.clear()
