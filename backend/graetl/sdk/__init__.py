"""The GraETL pipeline SDK.

A pipeline folder looks like this::

    pipelines/icu_admissions/
        pipeline.py                     # the standardized entry file
        pipeline.toml                   # configuration
        shared.graphlib                 # reusable node-flow functions (pipeline wide)
        modules/
            load_vital_signals/
                module.py               # 0-n @pipeline.module definitions
                module.toml             # metadata for this module folder
                cleanup.graph           # node-flow module (compiled later)
                helpers.graphlib        # node-flow functions local to this module

``pipeline.py`` owns the pipeline object, its lifecycle, and shared helper
functions that modules and node graphs can both call::

    from graetl.sdk import Pipeline, Entity

    pipeline = Pipeline(id="icu_admissions", title="ICU Admissions", stateful=True)

    @pipeline.setup
    def setup(ctx):
        ctx.resources["src"] = connect(ctx.setting("source.dsn"))

    @pipeline.entities
    def discover(ctx):
        for row in ctx.resources["src"].execute(...):
            yield Entity(row.id, source_updated_at=row.last_modified)

    @pipeline.function("severity_band")
    def severity_band(score):
        ...

``modules/<name>/module.py`` reaches the pipeline through ``get_pipeline()``::

    from graetl.sdk import get_pipeline

    pipeline = get_pipeline()

    @pipeline.module(version=1, depends_on=["extract"])   # name = folder name
    def transform(ctx, entity):
        band = ctx.fn("severity_band")(entity.data["severity"])
"""

from graetl.sdk.context import Context, EntityContext, RunContext
from graetl.sdk.errors import AbortRun, RetryEntity, SkipEntity
from graetl.sdk.caching import cached
from graetl.sdk.pipeline import Entity, Module, Pipeline, Task, get_pipeline, node

__all__ = [
    "Pipeline",
    "Entity",
    "Module",
    "Task",
    "get_pipeline",
    "node",
    "cached",
    "Context",
    "RunContext",
    "EntityContext",
    "SkipEntity",
    "RetryEntity",
    "AbortRun",
]
