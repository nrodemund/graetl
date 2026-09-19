"""Request models for the HTTP API."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class StartRunRequest(BaseModel):
    mode: Literal["incremental", "full", "retry-failed"] = "incremental"
    params: dict[str, Any] = Field(default_factory=dict)
    steps: list[str] | None = None
    entity_ids: list[str] | None = None
    profile: bool = False
    fail_fast: bool | None = None
    #: Modules of one execution layer to run at once (overrides the pipeline).
    parallel: int | None = Field(default=None, ge=1, le=64)
    #: Emit ctx.debug() output (only ever on for a deliberate debug run).
    debug: bool = False
    #: Stop after this many entities - a debug or profile sample.
    limit_entities: int | None = Field(default=None, ge=1)
    #: How to pick that sample.
    sample: Literal["first", "random"] = "first"

    def merged_params(self) -> dict[str, Any]:
        params = dict(self.params)
        if self.steps:
            params["steps"] = self.steps
        if self.entity_ids:
            params["entity_ids"] = self.entity_ids
        if self.profile:
            params["profile"] = True
        if self.fail_fast is not None:
            params["fail_fast"] = self.fail_fast
        if self.parallel is not None:
            params["parallel"] = self.parallel
        if self.debug:
            params["debug"] = True
        if self.limit_entities is not None:
            params["limit_entities"] = self.limit_entities
            params["sample"] = self.sample
        return params


class CreatePipelineRequest(BaseModel):
    id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    title: str | None = None
    description: str = ""
    template: Literal["stateful", "stateless", "empty"] = "stateful"


class CreateModuleRequest(BaseModel):
    """Scaffold pipelines/<id>/modules/<name>/."""

    name: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z][A-Za-z0-9_-]*$")
    title: str | None = None
    folder: str | None = None
    execution_layer: int = 0


class UpdatePipelineRequest(BaseModel):
    enabled: bool | None = None


class WriteFileRequest(BaseModel):
    content: str


class MoveFileRequest(BaseModel):
    """Rename or move one file or folder inside the pipeline."""

    source: str
    target: str
    #: Carry a renamed module's entity state over to the new name.
    migrate_state: bool = True


class CreateFolderRequest(BaseModel):
    path: str = Field(min_length=1, max_length=255)


class WriteGraphRequest(BaseModel):
    """The whole graph document, as the editor holds it."""

    graph: dict
    #: Compile straight away and report the result (the default).
    compile: bool = True


class PreviewGraphRequest(BaseModel):
    """An unsaved document to compile, so the editor can preview its own edits."""

    graph: dict


class CreateGraphRequest(BaseModel):
    """A new, empty graph."""

    path: str
    kind: str = "module"
    #: What the entry node hands the module: one entity, a batch, or nothing.
    entry: Literal["entity", "batch", "once"] = "entity"
    #: Entities per call for a batch graph; 0 uses the configured default.
    batch_size: int = 0
    title: str | None = None
    description: str = ""
    execution_layer: int = 0


class CreateFileRequest(BaseModel):
    path: str = Field(min_length=1, max_length=255)
    content: str = ""


class ResetEntityRequest(BaseModel):
    module: str | None = None


class ResetStateRequest(BaseModel):
    module: str | None = None
    drop_entities: bool = False
