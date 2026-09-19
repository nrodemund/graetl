"""Control-flow exceptions pipeline code can raise."""

from __future__ import annotations


class GraetlError(Exception):
    """Base class for GraETL errors."""

    #: When True the state store leaves the entity pending instead of failing it.
    graetl_pending = False


class SkipEntity(GraetlError):
    """Skip this entity for the current module.

    The state row is written as ``skipped`` and counted as processed, so the
    entity is not retried until its source revision or the module version
    changes.
    """

    def __init__(self, reason: str = "skipped") -> None:
        super().__init__(reason)
        self.reason = reason


class RetryEntity(GraetlError):
    """Leave the entity pending so the next run picks it up again."""

    graetl_pending = True

    def __init__(self, reason: str = "retry") -> None:
        super().__init__(reason)
        self.reason = reason


class AbortRun(GraetlError):
    """Stop the whole run cleanly (marked as failed unless ``ok=True``)."""

    graetl_pending = True

    def __init__(self, reason: str = "aborted", *, ok: bool = False) -> None:
        super().__init__(reason)
        self.reason = reason
        self.ok = ok


class PipelineDefinitionError(GraetlError):
    """The pipeline entry file is invalid."""


class StopRequested(GraetlError):
    """Raised internally when the operator requested a stop."""

    graetl_pending = True
