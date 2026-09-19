# GraETL architecture

## Processes

```
┌────────────────────────────────────────────┐
│ graetl serve   (uvicorn, one process)      │
│                                            │
│  Starlette API ──► Supervisor ──► spawns   │
│        │               │                   │
│    EventHub  ◄── output reader thread      │
│        │                                   │
│     WebSocket clients (UI)                 │
└────────────┬───────────────────────────────┘
             │ subprocess (one per run)
┌────────────▼───────────────────────────────┐
│ python -m graetl.runner run …              │
│   ControlWatcher  ── polls runs.control     │
│   Executor        ── setup, tasks,          │
│                      discovery, modules     │
│   EventWriter     ── JSONL on stdout        │
│   your pipeline.py                          │
└─────────────────────────────────────────────┘
```

The server never imports pipeline code. Even reading a pipeline's definition
(`Supervisor.inspect`) happens in a throw-away `python -m graetl.runner inspect`
process, so a syntax error, an import-time hang or a segfaulting database driver
can only kill that child.

## Databases

| File | Written by | Contents |
| --- | --- | --- |
| `pipelines/etl.db` | the **server only** | `pipelines`, `runs`, `run_steps`, `run_events`, `settings` |
| `pipelines/<id>/state.db` | the **runner only** | `entities`, `entity_module_state`, `pipeline_meta`, plus whatever tables the pipeline creates |

Single-writer per database keeps SQLite happy. The runner *reads* `etl.db` for
one thing only: the control word of its own run. Both databases run in WAL mode
with a busy timeout, so the UI can read while a run writes.

Schema changes are plain SQL scripts in `MIGRATIONS` lists, applied in order and
tracked in SQLite's `user_version`.

## The event protocol

The runner writes one JSON object per line to stdout, prefixed with
`@@GRAETL@@`. `sys.stdout`/`sys.stderr` are replaced inside the runner, so
anything pipeline code prints becomes a structured `log` event tagged with the
current step; a stray write to fd 1 by a C library arrives without the prefix
and is treated as a plain console line. The protocol can therefore never be
corrupted by noisy dependencies.

Event kinds: `run_start`, `step_start`, `step_end`, `entity`, `progress`, `log`,
`status`, `heartbeat`, `profile`, `run_end`.

The supervisor's reader thread does three things with every event: append it to
`pipelines/<id>/logs/run_NNNNNN.jsonl`, fold it into `etl.db`
(`run_steps`, progress, heartbeat, final metrics), and publish it to the
`EventHub`, which fans it out to WebSocket subscribers and keeps a replay
buffer so a console opened late still shows the whole run.

## Control flow

`runs.control` + `runs.control_seq` is the channel from server to runner.
`ControlWatcher` polls it every `control_poll_seconds` and flips a
`threading.Event`. The executor checks it:

* between entities and between steps (`Executor._guard`),
* wherever pipeline code calls `ctx.checkpoint()`.

Pause therefore never interrupts a transaction. Stop raises `StopRequested`,
which the state store recognises as an interruption and records as `pending`
rather than `failed`, so nothing is poisoned by an operator action.

## Loading a pipeline

`load_pipeline()` compiles `pipeline.py` from source (never through
`__pycache__`, and runner processes are spawned with `-B`, so a file edited in
the UI always takes effect), then walks every code directory below the pipeline
folder — skipping `data/`, `logs/`, `profiles/`, `.cache/`, `__pycache__` and
dot/underscore folders — collecting `*.module.py` files in a stable order. For
each one it:

1. merges `module.toml` (folder-wide) with `<name>.module.toml` (that module),
2. sets the "currently loading" context (pipeline + file + folder + metadata) so
   `get_pipeline()` works and `@pipeline.module()` can default its name to the
   file name,
3. executes the file with its own folder on `sys.path`, then drops the helper
   modules it imported from that folder back out of `sys.modules`, so two module
   files can both `import helpers` without colliding,
4. asserts the file defined exactly one module.

Plain `.py` files are never executed at load time — only when a module imports
them. An exception in a module file is re-raised as `<file>: <error>`, so the
registry and the UI name the file that broke. Each `*.graph` file is listed as a
pending module (name taken from the file), and a name collision between a
`.module.py` and a `.graph` is rejected.

## Ordering

Two independent mechanisms, both compiled into the same thing:

* `execution_layer` (int, default 0) — a stage number. `requirements_for(module)`
  expands it into "every module in a strictly lower layer, at its current
  version".
* `depends_on` — explicit edges, validated to never point at a higher layer.

`select_work()` turns those requirements into SQL: an entity is only offered to a
module when each required module has state `done` or `skipped` **at that
version**, with `processed_source_updated_at >= entities.source_updated_at`.
`skipped` counts as up to date — the module looked and had nothing to do.

With `cascade` (on by default) an entity also becomes due when a required module
processed it more recently than this one did, so bumping an upstream version
refreshes everything derived from it.

Running a subset of steps never pulls upstream modules in implicitly; the
executor checks them and reports what is behind (`waiting_for_upstream`).

## Execution order

1. `setup`
2. `@pipeline.task(phase="pre")` in definition order
3. entity discovery (stateful) — upsert into `entities`, optional soft-delete of
   entities that vanished from the source
4. modules in topological order (`module-major`), or entity by entity through
   the whole chain (`execution="entity-major"`)
5. `@pipeline.task(phase="post")`
6. `teardown` (always, even after a failure)

A pipeline with no steps and no entities walks straight through and succeeds,
with metrics showing zero work — this is a valid, useful state.

## Concurrency

`parallel_modules` (run param > `Pipeline(...)` > `pipeline.toml` >
`graetl.toml`) decides how many modules of one execution layer run at once. The
executor walks layers in order; inside a layer it splits modules into batches by
their intra-layer `depends_on` edges and runs each batch on a
`ThreadPoolExecutor`. Layers are barriers, so a higher layer never starts early.

Each module worker owns:

* its own `StateStore` — a separate SQLite connection, so `ctx.db` is never
  shared between threads,
* its own `EntityContext` — own metrics (merged into the run counters when the
  module finishes) and own resources,
* a row in `module_locks` for the module's whole execution, refreshed every 15 s.
  `acquire_module_lock` refuses a module another live worker holds and takes over
  one whose heartbeat is older than `LOCK_STALE_SECONDS` (90 s). This is what
  makes "one worker per module" hold across processes, not just threads.

Transaction mode follows the parallelism:

| modules at once | mode | behaviour |
| --- | --- | --- |
| 1 | `BEGIN IMMEDIATE` | write lock up front; no contention, no retries |
| >1 | `BEGIN` (deferred) | write lock at the first write; workers queue only around writes |

A deferred transaction that loses the race raises `LockConflict` **before any
state row is written**. `_run_entity` retries the entity (`lock_retries`, default
5, exponential backoff with jitter); anything still contended is counted and left
pending. Contention is therefore never recorded as a failure, and a retry is a
clean re-execution against an untouched database — the same at-least-once
contract that already applies to work outside `ctx.db`.

Entities are claimed in batches (`entity_batch_size`, default 500) paging forward
by `entity_id`, so memory is bounded and a failing entity cannot be re-selected
forever within one run.

`@pipeline.resource(name, close=...)` factories are called once per worker and
closed when the worker finishes; `ctx.resources` remains a shared dict for values
that are safe to share.

## Failure model

| What fails | Effect |
| --- | --- |
| one entity in one module | that entity's transaction rolls back, the state row is `failed` with the error, the run continues, the run ends as `failed` |
| a task | run aborts (`fail_fast` defaults to true for tasks) |
| `setup` | run fails before any module runs |
| the runner process dies | supervisor marks the run `crashed`; `running` state rows are healed to `pending` on the next run |
| the server dies | active runs are marked `crashed` at the next startup |

`Pipeline(max_attempts=n)` stops an entity from being retried forever in
`incremental` mode; `retry-failed` always picks it up again.
