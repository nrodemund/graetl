"""In-process pub/sub used to fan events out to WebSocket clients."""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import Any, Iterable


class EventHub:
    """Topic based broadcast with a small replay buffer per topic.

    Topics used by GraETL:
      ``system``          - pipeline/run lifecycle for the whole UI
      ``run:<id>``        - console + progress for one run
    """

    def __init__(self, buffer_size: int = 500) -> None:
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._buffers: dict[str, deque] = defaultdict(lambda: deque(maxlen=buffer_size))
        self._buffer_size = buffer_size
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    # --------------------------------------------------------------- publish

    def publish(self, topic: str, message: dict[str, Any]) -> None:
        """Publish from the event loop thread."""
        self._buffers[topic].append(message)
        for queue in list(self._subscribers.get(topic, ())):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:  # pragma: no cover - slow client
                pass

    def publish_threadsafe(self, topic: str, message: dict[str, Any]) -> None:
        """Publish from any other thread (the runner output reader)."""
        loop = self._loop
        if loop is None or loop.is_closed():
            self._buffers[topic].append(message)
            return
        try:
            loop.call_soon_threadsafe(self.publish, topic, message)
        except RuntimeError:  # pragma: no cover - loop shutting down
            self._buffers[topic].append(message)

    # ------------------------------------------------------------- subscribe

    def subscribe(self, topic: str, *, maxsize: int = 1000) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._subscribers[topic].add(queue)
        return queue

    def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        subs = self._subscribers.get(topic)
        if subs:
            subs.discard(queue)
            if not subs:
                self._subscribers.pop(topic, None)

    def replay(self, topic: str, limit: int | None = None) -> list[dict[str, Any]]:
        items = list(self._buffers.get(topic, ()))
        return items[-limit:] if limit else items

    def drop(self, topics: Iterable[str]) -> None:
        for topic in topics:
            self._buffers.pop(topic, None)
