# GraETL

**Gra**phical **ETL** — a tool for orchestrating, running and developing ETL pipelines.

Python backend, browser console, SQLite inside. Every run happens in its own OS
process, so pipeline code can never take the server down, and a run can be
paused, resumed and stopped safely at any time.

---

## Quick start

```bash
# 1. install (uv recommended)
uv sync                      # or: python -m venv .venv && pip install -e ".[data,dev]"

# 2. start the server + UI
uv run graetl serve          # http://127.0.0.1:8777
```

Open <http://127.0.0.1:8777>. Three demo pipelines ship with the project:

| Pipeline | Kind | What it shows |
| --- | --- | --- |
| `icu_admissions` | stateful | A live source system whose records keep changing. Run it twice — only the changed admissions are reprocessed. |
| `sales_import` | stateless | A classic Excel/CSV drop-folder ETL with metrics and rejected rows. |
| `blank` | empty | A pipeline with no modules and no entities. It succeeds immediately — that is allowed, and the metrics show that nothing had to be done. |

### Command line

```bash
graetl serve                     # server + UI
graetl list                      # discovered pipelines
graetl new my_flow --template stateful
graetl new-module my_flow load_vital_signals   # modules/load_vital_signals/load_vital_signals.module.py
graetl run icu_admissions        # run in the foreground (developer mode, same process)
graetl run icu_admissions --mode full --profile
graetl runs icu_admissions       # run history
graetl state icu_admissions      # entity/module state summary
graetl reset icu_admissions --module transform
graetl inspect icu_admissions    # pipeline definition as JSON
```

---

## The `pipelines/` folder is the project

Everything portable lives in one folder. Copy it, back it up, put it in git —
that is the whole installation state.

```
pipelines/
├── etl.db                          # internal database: registry, runs, metrics, control
└── icu_admissions/                 # one folder per pipeline
    ├── pipeline.py                 # the standardized entry file
    ├── pipeline.toml               # configuration (ctx.config / ctx.setting("a.b"))
    ├── shared.graphlib             # library of node-flow functions, pipeline wide
    ├── modules/
    │   └── vitals/                      # group modules however you like
    │       ├── load_signals.module.py   # ONE module, named after the file
    │       ├── load_signals.module.toml # its metadata
    │       ├── parsing.py               # helper - imported, never auto-loaded
    │       ├── vitals.nodes.py          # node library - every function is a node
    │       ├── module.toml              # defaults for the whole folder
    │       ├── cleanup.graph            # node-flow module (a module too)
    │       └── helpers.graphlib         # library of node-flow functions
    ├── state.db                    # entity state (stateful pipelines only)
    ├── data/                       # whatever the pipeline reads and writes
    ├── logs/run_000123.jsonl       # console output per run
    └── profiles/                   # cProfile dumps from profiled runs
```

**One file, one module.** Every `*.module.py` anywhere below the pipeline folder
is loaded automatically and defines exactly one module, named after the file.
Every `*.graph` is a module too: it compiles to the `*.module.py` beside it, and
from there it is an ordinary module. Plain `.py` files are helpers: they run only when
the module that needs them imports them, which keeps loading cheap and makes
"run just this one module" predictable.

Folders are free grouping. A folder can hold several modules
(`dosomething.module.py`, `dosomethingwhatever.module.py`, `dosomething2.graph`),
their helper code and their graphs.

---

## Writing a pipeline

### `pipeline.py` — the entry file

It owns the pipeline object, its lifecycle, and the shared functions modules
call. The standard run/resume/pause/profiling machinery comes with it; you only
write the ETL logic.

```python
from graetl.sdk import Entity, Pipeline

pipeline = Pipeline(id="icu_admissions", title="ICU Admissions", stateful=True)


@pipeline.resource("src", close=lambda c: c.close())   # one per worker
def src(ctx):
    return connect(ctx.setting("source.dsn"))


@pipeline.setup                       # once per run, before anything else
def setup(ctx):
    ctx.db.execute("CREATE TABLE IF NOT EXISTS fact (...)")


@pipeline.entities                    # what this pipeline works on
def discover(ctx):
    for row in ctx.resource("src").execute("SELECT id, last_modified FROM admissions"):
        yield Entity(id=row.id, source_updated_at=row.last_modified)


@pipeline.function("severity_band")   # shared helper: modules call it via ctx.fn(),
def severity_band(score):             # node graphs will expose it as a node
    return "critical" if score >= 5 else "normal"


@pipeline.task("notify", phase="post")  # run-scoped step, no entity state
def notify(ctx):
    ctx.info("done")


@pipeline.teardown                    # always runs, even after a failure
def teardown(ctx):
    ctx.info("done")                  # resources are closed for you
```

### `<name>.module.py` — the modules

```python
# modules/transform/transform.module.py
from graetl.sdk import get_pipeline

import parsing                    # helper file next to this one, loaded on import

pipeline = get_pipeline()


@pipeline.module(version=2, execution_layer=20)
def transform(ctx, entity):
    ctx.checkpoint()                                    # honours pause / stop
    band = ctx.fn("severity_band")(entity.data["severity"])
    ctx.db.execute("INSERT OR REPLACE INTO fact ...")   # commits with the state row
    ctx.metric("rows_written", 1)
```

```toml
# modules/transform/transform.module.toml — optional metadata, shown in the UI
title = "Transform & load"
description = "Derive length of stay and severity band"
tags = ["core"]
owner = "data-engineering"
# execution_layer = 20
```

A plain `module.toml` (no name prefix) sets defaults for every module in that
folder; `<name>.module.toml` overrides them for one module.

Bump `version=` whenever a module's logic changes — every entity is then
reprocessed by that module, and everything downstream of it.

Create one with `graetl new-module <pipeline> <name>`, or the **+ Module**
button in the pipeline's Files tab.

Helper imports resolve inside the module's own folder, so `import parsing` finds
`modules/transform/parsing.py`. Each module file gets its own copy of those
helpers, so two modules in different folders can both `import helpers` without
colliding.

## Ordering: execution layers

`execution_layer` is the simple ordering knob — a stage number, nothing to wire
up:

```python
@pipeline.module(version=1, execution_layer=10)   # fluid_intake
@pipeline.module(version=1, execution_layer=10)   # creatinine
@pipeline.module(version=3, execution_layer=20)   # calc_apache_4
```

Before a module touches an entity, **every module in a lower layer must be up to
date for that entity** — processed at its current version, at or beyond the
entity's current source revision. So `calc_apache_4` in layer 20 automatically
waits for all of layer 10, without naming a single dependency. Modules sharing a
layer are independent and run in file order.

`depends_on=["extract"]` still exists for fine-grained edges (inside a layer, or
to be explicit). A dependency may never point at a *higher* layer — that can
never be satisfied, so GraETL refuses to load it.

Two consequences worth knowing:

* **Bumping an upstream module refreshes what follows it.** If layer 10 is
  reprocessed for an entity, everything above it is reprocessed too, so derived
  data never goes stale. Turn it off with `Pipeline(..., cascade=False)`.
* **Running one module for debugging stays literal.** `graetl run <p> --steps
  calc_apache_4` executes exactly that module. It does not silently pull its
  lower layers in — it checks them, skips entities whose upstream is behind, and
  says so in the console.

### Stateless vs stateful

* **Stateless** (`Pipeline(..., stateful=False)`) — ordered `@pipeline.task`
  steps. The run records its metrics (rows read/written, durations, counters),
  but keeps no per-entity state. Good for "load this Excel file".
* **Stateful** (`@pipeline.entities` + module folders) — GraETL keeps an entity
  state database, which is what makes a pipeline resumable and incremental.

A pipeline can mix both: tasks run before (`phase="pre"`, the default) and after
(`phase="post"`) the entity modules.

### The `ctx` object

| | |
| --- | --- |
| `ctx.config`, `ctx.setting("source.dsn", default)` | `pipeline.toml` |
| `ctx.dir`, `ctx.data_dir`, `ctx.path("data/in.csv")` | paths inside the pipeline folder |
| `ctx.db` | the pipeline's SQLite connection (see *Transactional safety*) |
| `ctx.fn("name")` | a shared function registered with `@pipeline.function` |
| `ctx.resource("name")` | this worker's own connection/session (see *Running modules in parallel*) |
| `ctx.state` | the `StateStore` for advanced queries |
| `ctx.resources` | your own connections/handles, shared across steps |
| `ctx.info/warn/error/success/debug(msg, **data)` | structured console output |
| `ctx.metric("rows_written", n)`, `ctx.gauge(...)` | run counters shown in the UI |
| `with ctx.timeit("query"): ...` | block profiling |
| `ctx.progress(done, total)` | progress bar |
| `ctx.checkpoint()` | pause/stop yield point — call it in long loops |
| `ctx.skip("reason")` | mark this entity as skipped for this module |
| `raise RetryEntity()` | leave it pending for the next run |
| `raise AbortRun("...")` | stop the whole run cleanly |

Anything the pipeline `print()`s also lands in the console, tagged with the step
that printed it.

## Running modules in parallel

A layer is a natural parallelism boundary: everything in it is independent by
definition. Set how many modules of one layer may run at once, and GraETL runs
that layer with that many workers, then moves to the next layer:

```python
pipeline = Pipeline(id="icu", stateful=True, parallel_modules=4)
```

```toml
# graetl.toml — the default for every pipeline
[runtime]
parallel_modules = 4
```

```bash
# or per run
POST /api/pipelines/icu/runs  {"parallel": 4}
```

The rules that keep this safe:

* **One worker per module, ever.** A module is claimed in `module_locks` for the
  duration, with a heartbeat. A second worker — in this run, another run, or a
  `graetl run` you started in a terminal — is refused, and a lock whose worker
  died is taken over after 90 s. A module is never executed twice at the same
  time.
* **One connection per worker.** Each module worker opens its own SQLite
  connection, so `ctx.db` is never shared between threads.
* **Layers are barriers.** A higher layer only starts once the lower ones are
  done, and per entity the up-to-date check still applies. `depends_on` edges
  *inside* a layer are honoured too: those modules run in a later batch.
* **Contention never corrupts and never fails.** When modules run in parallel
  their transactions take the write lock at the first write rather than up
  front, so workers only queue around writes. If a worker loses the race, its
  entity is rolled back whole, **no state row is written**, and it is retried a
  few times; anything still contended stays pending for the next run. A lock
  conflict is never recorded as a failure.
* **Entities are claimed in batches**, paging forward by entity id, so a run
  never loads the whole backlog into memory and always terminates.

### Resources must be per worker

A database connection or HTTP session is usually not safe to share between
threads. Register a factory and GraETL builds one per worker, then closes it:

```python
@pipeline.resource("source", close=lambda conn: conn.close())
def source(ctx):
    return sqlite3.connect(ctx.setting("source.dsn"))
```

```python
row = ctx.resource("source").execute(...)      # this worker's own connection
```

`ctx.resources` is still there — a plain dict shared by every worker. Fine for
immutable values, unsafe for connections once `parallel_modules > 1`.

---

## How resuming works

`state.db` has two tables:

**`entities`** — one row per business entity, with `source_updated_at`: the
**source revision**, i.e. the moment the entity last changed in the source
system. In live systems (an ICU admission gets discharged, a patient record is
corrected) that value moves forward — that is what makes an already processed
entity dirty again. Any comparable token works: a timestamp, a version number,
an ETag.

**`entity_module_state`** — one row per (entity, module), recording

* `module_version` — which version of the module produced this state,
* `processed_source_updated_at` — **which source revision was processed**,
* `processed_at`, `attempts`, `status`, `error`, `duration_ms`, `run_id`.

A module has work to do for an entity when **any** of these is true:

1. there is no state row yet,
2. the row is not `done`,
3. the module's `version=` was bumped (change the logic → bump the version →
   only that module reprocesses everything),
4. `processed_source_updated_at < entities.source_updated_at` — the source moved.

Plus: a module with `depends_on=[...]` only runs for entities whose upstream
modules are `done`.

### Run modes

| Mode | Selects |
| --- | --- |
| `incremental` (default) | exactly the work items from the rules above |
| `full` | every entity, regardless of state |
| `retry-failed` | only entities whose state is `failed` |

### Transactional safety

Module code writes through `ctx.db` — the *same* SQLite connection that holds
the state table. GraETL wraps each (entity, module) execution in one
transaction that covers both the module's data writes and the state row update:

```
BEGIN IMMEDIATE
  <your INSERT/UPDATE statements>
  UPDATE entity_module_state SET status='done', processed_source_updated_at=…
COMMIT
```

So "state says done but the data was never written" cannot happen, and a
rolled-back entity is recorded as `failed` with its error while every other
entity's work stays committed. Writes to systems *outside* that connection
(a remote database, an API) are at-least-once: the state row only flips to
`done` after the module returns, so an interrupted entity is retried on resume.

Interrupted work (`status = running` left behind by a killed process) is reset
to `pending` at the start of the next run.

---

## Running, pausing, stopping

Each run is a separate process:

```
graetl serve  ──spawns──▶  python -m graetl.runner run --pipeline X --run-id N
      ▲                             │
      │  runs.control in etl.db     │  JSONL events on stdout
      └─────────── pause/resume/stop ┘  (console, progress, metrics)
```

* **Pause** writes a control word into `etl.db`. The runner picks it up within
  half a second and blocks at the next safe point — between entities, between
  modules, or wherever your code calls `ctx.checkpoint()`. The process stays
  alive with its connections open; **Resume** continues instantly.
* **Stop** asks the run to finish the current entity and exit. If it does not
  exit within `stop_grace_seconds` (default 20), the process tree is killed —
  `taskkill /F /T` on Windows, process-group signals elsewhere.
* Resuming a run that has already finished (stopped, failed, crashed) starts a
  **new run** in `incremental` mode that picks up exactly what is left; the new
  run keeps a link to its parent.
* If the server dies while runs are active, those runs are marked `crashed` at
  the next startup and their entity state is healed on the next run.
* Only one active run per pipeline — a second start returns HTTP 409.

---

## Working on a pipeline

### The entity browser

The Entities tab is built for real volumes: search, filters and paging all run in
SQL, so 100k entities page as fast as 100. Filter by module and state, sort by id
/ last seen / newest revision, and click any row to see exactly what each module
did to that entity — its version, which source revision it processed, when, how
many attempts, how long it took and the error if there was one. From there you
can reset one module's state for that entity, or debug-run that module against
it.

### The editor

The Files tab runs **Monaco** — the editor from VS Code: real Python
intelligence, multi-cursor, find & replace, the command palette on `F1`, a
minimap, and `Ctrl+S` to save. Each file keeps its own undo history and scroll
position, so switching between them costs nothing. Saving compiles the file
first — a syntax error is refused and squiggled on the offending line, so a
broken module can never reach the registry. **Check** validates without saving.
Editing is blocked while a run is active.

### The file tree

The Files tab is a proper tree, and it behaves like one:

| | |
| --- | --- |
| **Click** | open · **double-click** or `F2` renames in place, with the stem preselected |
| **Drag** | move a file or folder somewhere else in the pipeline |
| **Drop from the desktop** | adds files — whole folders too, structure intact |
| **Right-click** | open · rename · duplicate · new file/folder/module/graph/function graph/node library · copy path · delete |
| **Keys** | `↑ ↓` walk, `← →` collapse and expand, `Enter` open, `F2` rename, `Del` delete |

Icons say what a file *is* to GraETL, not just its extension: module, graph,
function graph, generated, config, data. Drag the divider to resize the pane.

Two rules run through every operation, and the server enforces them so the UI
cannot disagree:

* **A generated `.py` belongs to its graph.** Renaming, moving or deleting it
  directly is refused, and the error points at the graph. Act on the graph and
  the `.py` follows — including the name inside the graph document, so the
  module is renamed too and everything recompiles.
* **A module is named by its file, and entity state is keyed by module name.**
  Renaming a module therefore *migrates its state*: 27,000 entities stay `done`
  and nothing reprocesses, and the response says how many rows moved. Deleting a
  module drops its state rather than leaving orphaned rows in the entity
  browser. Moving a *folder* renames nothing, so it never touches state.

Nothing can escape the pipeline folder, and every operation is refused while a
run is active.

#### Where Monaco comes from

The browser looks for it in this order:

1. `backend/graetl/server/static/vendor/vs` — a copy inside the install
2. `[ui] monaco_url` in `graetl.toml` (the jsDelivr CDN by default)
3. the small built-in editor, if neither is reachable

So nothing breaks without internet — but for the full editor on an offline or
air-gapped machine, vendor it once:

```bash
graetl vendor-monaco                                      # downloads it
graetl vendor-monaco --from node_modules/monaco-editor    # from an npm install
```

Setting `monaco_url = ""` forces the built-in editor and never touches the
network.

### Direct manipulation

Rows are the objects, so you act on them directly:

| | |
| --- | --- |
| **Double-click a module** in Steps | opens its `.module.py` in the editor |
| **Double-click a module folder** | opens the first module in it |
| **Right-click a module** | run · debug run · profile run · edit source · reset its state |
| **Right-click an entity** | open details · copy id · filter to it · reset its state |
| **Right-click a pipeline** in the sidebar | open · code · entities · run · retry · enable/disable |
| `/` | focus the entity search |
| `o` `r` `e` `f` | jump to Overview / Runs / Entities / Files |
| `Esc` | close the menu, drawer or dialog |
| `Ctrl+S` | save the open file |

### Node graphs

A module can be **drawn** instead of typed. A `.graph` file is a node graph in
the Blueprint sense - execution wires in white, typed data wires in colour,
branches and loops as real nodes - and it **compiles to ordinary Python** that
lands next to it:

```
modules/vitals/load.graph        ->  modules/vitals/load.module.py
helpers.graphlib                 ->  helpers.graphlib.py   (all its functions)
```

Nothing interprets a graph at run time. By the time the pipeline runs there is
only Python, which the normal loader picks up without knowing a graph was
involved. Compilation happens automatically whenever a graph is newer than its
output, so editing and running needs no separate step; `graetl compile <id>`
does it by hand and `graetl graph <id> <file>` prints the result without writing.

If you ever want to stop drawing a module, delete the `.graph` and keep the
`.py`. That is the entire migration.

#### Where nodes come from

Four sources, one palette:

| | |
| --- | --- |
| **A node library** | A file named `<name>.nodes.py`. **Every public function in it is a node** — no decorator, no registration. This is the easy way to add nodes. |
| **A decorated function** | `@pipeline.function(...)` in `pipeline.py`. Pins come from the signature; `ctx` is implicit and never drawn. `pure=True` gives it no execution pins, `category=` groups it in the palette. |
| **A function graph** | A function in a `.graphlib` **library** — declared inputs and outputs, drawn on the canvas. Callable from any other graph, and from hand-written Python as `ctx.fn("name")`. |
| **True reflection** | `py:math.floor`, `py:pandas.read_csv`, `py:len` - any importable callable. `inspect.signature` gives the pins and the return type, and **⤓ Import…** in the palette lets you browse a module rather than recall a name. |

Plus the built-ins: `core:branch`, `core:for_each`, `core:while`,
`core:sequence`, operators, literals, dict/list builders, f-strings, graph
variables, and the GraETL calls (`ctx.log`, `ctx.metric`, `ctx.skip`,
`ctx.checkpoint`, `ctx.db.execute`).

#### Importing from Python, visually

`py:pandas.read_csv` works — if you already know it exists. **⤓ Import…** in the
node palette is the other half: type a module name and it shows you what is in
it, with signatures and one-line docs.

```
Import a Python module          pandas            [Open]
  ƒ read_csv   (filepath_or_buffer, sep=…, …)   Read a comma-separated values file into DataFrame.
  ƒ concat     (objs, *, axis=…, join=…, …)     Concatenate pandas objects along an axis.
  C DataFrame  (data=…, index=…, columns=…)     Two-dimensional, size-mutable, tabular data.
  ▸ pandas.io                                    (step into it)
```

* Filtering ranks **name matches first**, so a doc mentioning `read_csv` never
  outranks `read_csv` itself.
* A package lists its submodules as something to step into, with breadcrumbs
  back out.
* Picking a function places it as a `py:` node, pins already resolved.
* Picking a **class** offers two things: place the constructor, or *browse its
  methods*.

That last one matters, because `df.to_csv(path)` is a call on a value and no
module-level name will ever reach it. Picking a method places a **method node**
— `object.name(…)` — with an `object` pin, as many argument pins as you asked
for, and the keyword arguments you named. So the whole shape the palette
suggests reads as one chain:

```python
frame = pandas.read_csv(ctx.setting("source.path"))
for row in frame.itertuples():
    ...                                  # do something with the entity
frame.to_csv(out_path, index=False)
```

A method node is impure by default, so it sits in the execution chain and is a
statement. Tick `pure` in its config when it only computes something, and it
folds into the expression that uses it instead.

`[graphs] reflect_allow` applies here too: with it set, the browser will only
import the roots it lists.

#### What a module runs for

A module graph starts with one of three entry nodes, and that single choice
decides how the runner calls it. You are asked when you create the graph, and
the entry node's own right-click menu changes it later:

| Entry | The module | When |
| --- | --- | --- |
| **Each entity** | `fn(ctx, entity)` | The default. One call per entity, one transaction each — this is what "resumable at entity granularity" means. |
| **A batch of entities** | `fn(ctx, entities)` | When the work is cheaper in bulk: one `INSERT` of 500 rows instead of 500 of one. |
| **Once per run** | `fn(ctx)` | No entity at all. For work about the whole table: rebuild a summary, export the lot, vacuum. |

**A batch is one transaction.** Every entity in it is marked done together, or
none is and the whole batch is retried next run — the same promise a single
entity already gets, which is why a batch module cannot report a per-entity
outcome. Set the size on the entry node; it defaults to `entity_batch_size`.

**A once module** sits in the execution-layer order like any other, so
everything below its layer is up to date by the time it runs. It keeps no entity
state, so it runs on every run and is never a *per-entity* requirement for the
modules above it — the layer barrier still orders it, but it cannot gate an
entity it knows nothing about.

In hand-written Python these are `scope=` on the decorator:

```python
@pipeline.module(version=1, execution_layer=20, scope="batch", batch_size=500)
def load_rows(ctx, entities): ...

@pipeline.module(version=1, execution_layer=40, scope="once")
def rebuild_summary(ctx): ...
```

#### Function graph libraries: `*.graphlib`

A `.graphlib` file holds **one or more** function graphs and compiles all of
them into the single `.py` beside it. Grouping is organisation, not scope: each
function still registers under its own name, so `graph:unit_of` means one
function wherever it is drawn.

Opening a library puts a tab per function above the canvas, with **＋** to add
one and a right-click menu on each tab for rename, duplicate, signature and
delete. The file is what gets saved, whichever tab is showing — so an edit in
one function and an edit in another are one save, and the dirty marker belongs
to the file.

**Signature…** on a tab edits what the function takes and returns:

```
Inputs   value: float, unit: str        name, or name: type — comma separated
Outputs  grams: float
Pure     no side effects, so callers need no execution pins
Cache    call it once per distinct set of arguments
```

Files written before libraries existed are read unchanged — a single function
at the top level is a library of one, and saving writes the list form.

#### Caching a function

Some functions answer the same question over and over. `unit_of(123)` asks the
database what unit parameter 123 is recorded in; `load_parameters()` builds the
same dictionary from the same file. Across 27,000 entities that is 27,000 round
trips for one answer.

Tick **Cache results** on a function graph, or write `@node(cache=True)` on a
function in a node library, and the result is memoised:

```python
@node(cache=True)                     # or cache=1000 for a specific size
def unit_of(ctx, code):
    return ctx.db.execute("SELECT unit FROM d_references WHERE id = ?", (code,)).fetchone()[0]
```

The rules are chosen so it needs no further thought:

* **Per process.** Every run is its own OS process, so a cache never outlives a
  run and can never go stale between runs. Inside a run every module shares it.
* **`ctx` is not part of the key.** It is the run context; keying on it would
  defeat the cache entirely.
* **Types are part of the key**, so `get(1)`, `get(True)` and `get("1")` are
  three different questions — Python would otherwise treat the first two as one.
* **An unhashable argument does not raise.** Lists, tuples, sets and dicts are
  frozen recursively so they still cache; anything genuinely unhashable simply
  calls through every time. A cache is an optimisation, never a reason a
  pipeline fails.
* **Exceptions are not cached** — a failure is retried next time.
* Bounded at 4096 entries per function by default (`[runtime] cache_size`, or
  per function), oldest evicted first.

Hits and misses appear in the run metrics and in the test-run pane, so "did the
cache do anything" is a question with an answer.

One thing to know: a cached function hands back *the same object* every time.
If a caller mutates the dictionary it was given, the next caller sees the
change. Return something the caller has no reason to modify, or do not cache it.

#### Node libraries: `*.nodes.py`

Write plain functions; they are all nodes.

```python
# modules/vitals/vitals.nodes.py
from graetl.sdk import node


def bmi(weight_kg, height_m):
    """One line here becomes the node's description in the palette."""
    return weight_kg / (height_m ** 2)


@node(category="Risk", title="Risk score")
def risk(age, bmi_value):
    return math.log1p(age) * bmi_value


@node(pure=False)
def stamp(ctx, entity, value):        # takes ctx, so it is impure anyway
    ctx.db.execute("UPDATE ...", (value, entity.id))


def _round_half(x):                   # leading underscore: a helper, not a node
    ...
```

The rules are all defaults you can override:

* **Name** is the function name; the **pins** are its parameters and its return.
* **Purity** follows the signature: take `ctx` and the node is impure (it sits
  in the execution chain, because it can reach the database, the log and the
  run); don't, and it is pure (no execution pins, inlined into the expression
  that uses it). `@node(pure=...)` settles it either way.
* **Category** in the palette is the file name — `vitals.nodes.py` → *Vitals*.
* Names starting with `_`, anything the file merely imported, and
  `@node(skip=True)` are not nodes.

`＋ → New node library` creates one with that example inside. A node library is
loaded before the graphs and modules that use it, and two libraries defining the
same name is an error naming both files.

A node is **pure** (no execution pins, folded into the expression that uses it)
or **impure** (sits in the execution chain, becomes a statement). Reflected
nodes are pure only when they come from a module known to be side-effect free -
inlining something that writes to a file would quietly reorder it.

#### Editing a graph

Opening a `.graph` or `.graphlib` in the Files tab swaps the text editor for the
canvas. The gestures are Unreal's, because that is what the muscle memory
expects:

| | |
| --- | --- |
| **Left-drag** on empty space | marquee-select |
| **Right-drag** / middle-drag | pan (a right *click* opens a menu) |
| **Scroll** | zoom around the cursor |
| **Drag a node's header** | move it, with the rest of the selection; the node you grab comes to the front |
| **Drag a pin** | wire it — compatible pins light up, impossible ones dim |
| **Drag a wired input away** | pick that wire up and drop it somewhere else |
| **Drag a pin onto empty space** | the palette, filtered to what that pin can drive — and it wires the node it creates |
| **Alt-click a pin** | break every link on it |
| **Double-click** empty space | the node palette, at the cursor |
| **Double-click** a title, or **F2** | rename the node — the title becomes the variable name in the output |
| **Click an unconnected input** | type a literal (JSON, so `3`, `"high"` and `true` keep their types) |
| **C** | a comment box around the selection (right-click it for a colour) |
| **Right-click** anything | the menu for it: node, pin, wire, comment, canvas |

Ctrl+Z / Ctrl+Shift+Z undo and redo, Ctrl+C / Ctrl+V / Ctrl+D copy, paste and
duplicate, Del deletes, and dropping a node snaps it to an 8 px grid. The node
you grab comes to the front, so one dropped on another never buries it.

**Drag a pin into empty space** and the palette opens knowing where the wire
came from. It offers the things you would plausibly do with that value, first:
drag `entity` and you get `.id`, `.data`, `.label` and `.source_updated_at`
— read off the class, so they cannot drift — then the operations that fit its
type; drag a number and you get the operators and the casts; drag an execution
pin and you get the flow nodes. Pick one and it is placed *and wired*.

The palette otherwise searches every node the pipeline can offer. Type a dotted
name - `math.floor`, `statistics.fmean` - and reflection is offered as its own
entry: the server resolves the signature and the node arrives with its pins
already on it. The handful of built-ins whose pins depend on their settings
(`get_attr`, `make_dict`, `format`, the operators) have **Configure…** in their
menu; changing a setting re-resolves the node and drops any wire whose pin is
gone. The entry node is not in the palette at all: a graph has exactly one, it
is the function's signature, and the server refuses a document with two.

A comment box carries the nodes sitting on it, as Blueprint does. When you only
want to rearrange the boxes, Alt-drag skips it for one drag, or turn off **Drag
the nodes with it** in the box's menu to make it permanent — the box then draws
with a dashed edge, and the setting is saved with the graph.

**Nothing is saved until you ask.** Ctrl+S or Save writes the document and
recompiles it, and the result comes back as the errors and warnings for that
build - compiling half an edit is just noise. The *Generated Python* tab
compiles whatever the canvas is holding, saved or not, so you can check the
output before committing to it. Saving is refused while a run is active, for
the same reason editing a module file is.

#### Testing a graph without leaving it

**▶ Test** saves the graph, compiles it, and runs *just this module* for one
random entity with `ctx.debug()` switched on. The output streams into a pane
under the canvas — logs, per-entity results, and the error with the step it came
from — so the loop is draw, test, fix without ever changing tab. Right-click the
button for ten entities, every entity that needs it, or one specific id.

If the graph does not compile, nothing is run and the build errors say why.

#### The generated Python is the point

This is what a graph with a loop, a branch and a shared value compiles to -
unedited:

```python
@pipeline.module(version=1, execution_layer=10)
def score_entity(ctx, entity):
    """Band the entity's score, average its readings and record the result."""
    # Graph variables
    total = 0.0

    data = entity.data
    readings = data.get("readings", [])
    band = ctx.fn("severity_band")(ctx, data.get("score", 0))
    entity_id = entity.id

    # Running total of the readings, kept as a graph variable.
    for reading in readings:
        total = total + reading

    # High-risk cases get a warning and a counter.
    if band == "high":
        ctx.warning(f"{entity_id} is {band} risk")
        ctx.metric("high_risk")
    ctx.fn("record")(ctx, entity_id, band, total)
```

Three things make that readable rather than machine-shaped:

* **Pure nodes are expressions.** Operator precedence is tracked, so `a + b * c`
  comes out as `a + b * c` and not `(a + (b * c))`.
* **A shared value becomes a named local, at the right depth.** The compiler
  works out the deepest block that encloses every use, so a value needed in two
  branches is computed once above them - not duplicated into each.
* **Names come from meaning.** `entity.data` becomes `data`, `row.get("readings")`
  becomes `readings`, and nothing is ever called `get_attr_2`. Title a node and
  that title becomes the variable name.

Comment boxes land in the output as real comments, above the code they enclose.

#### What the compiler refuses

Scope is checked rather than assumed, so mistakes surface as errors naming the
node and the pin instead of a `NameError` halfway through 27,000 entities:

* reading a loop's `item` after the loop has finished
* reading an impure node's output before its execution pin has run
* one execution pin driving two nodes (use a Sequence)
* two wires into one data pin
* a node whose function, graph or import no longer exists

Type mismatches are **warnings**, never errors - Python is dynamic and the
author may know better than the annotation.

#### Configuration

```toml
[graphs]
# Reflected callables from these modules are pure by default.
pure_modules = ["math", "statistics", "json", "re", "datetime"]
# When set, only these roots may be reflected at all.
reflect_allow = ["math", "statistics"]
```

### Run, debug, profile

Every module row in the Steps table carries three buttons:

| | | |
| --- | --- | --- |
| ▶ | **Run** | that module only, for every entity that needs it |
| 🐞 | **Debug run** | one entity — a specific id or a random one — reprocessed even if it is up to date, with `ctx.debug()` output switched on |
| ⏱ | **Profile run** | a sample of N entities under cProfile; the dump lands in `profiles/` and the top functions appear in the console |

`ctx.debug()` is **only** emitted on a debug run. Everywhere else it costs
nothing, so you can leave verbose tracing in your modules permanently.

```bash
graetl run icu_admissions --steps transform --mode full --limit 1 --sample random --debug
graetl run icu_admissions --steps transform --limit 25 --sample random --profile
```

### Live process view

While a run is alive the runner samples itself every 2 seconds and streams
memory, CPU, thread count, throughput and parallelism into the run page, with
sparklines for memory and CPU. `psutil` is used when installed (`uv sync --extra
telemetry`) and the standard library otherwise, so the panel works on a bare
install.

---

## The UI

One console, at `backend/graetl/server/static/`: plain ES modules, served
straight from the package. **No build step, no Node, no dependencies** — it
works the moment `uv sync` finishes, and it is the same UI in a container, on a
server and on an air-gapped machine.

That is a deliberate trade. A framework would buy component ergonomics; what it
costs is a toolchain between you and a one-line UI fix, and a second thing to
keep in step with the API. At 43 KB gzipped over localhost there is nothing left
to win by bundling — the console loads in about 100 ms.

The only optional asset is Monaco, which is lazy-loaded on the Files tab and can
be vendored for offline use (`graetl vendor-monaco`).

---

## HTTP API

| Method | Path | |
| --- | --- | --- |
| `GET` | `/api/health`, `/api/overview` | |
| `GET` | `/api/pipelines`, `/api/pipelines/{id}` | |
| `POST` | `/api/pipelines` | scaffold a new pipeline |
| `POST` | `/api/pipelines/{id}/modules` | scaffold a module folder |
| `POST` | `/api/pipelines/sync` | rescan the folder |
| `PATCH`/`DELETE` | `/api/pipelines/{id}` | enable/disable, remove |
| `GET`/`PUT` | `/api/pipelines/{id}/file?path=…` | read/write pipeline code |
| `GET` | `/api/pipelines/{id}/entities` | entity state |
| `POST` | `/api/pipelines/{id}/state/reset` | clear processing state |
| `POST` | `/api/pipelines/{id}/runs` | start a run (`mode`, `steps`, `entity_ids`, `profile`) |
| `GET` | `/api/runs`, `/api/runs/{id}`, `/api/runs/{id}/steps`, `/api/runs/{id}/console` | |
| `POST` | `/api/runs/{id}/pause` · `/resume` · `/stop?force=` | |
| `WS` | `/api/ws/system` | pipeline + run changes |
| `WS` | `/api/ws/runs/{id}` | live console with replay |

---

## Configuration

`graetl.toml` in the project root:

```toml
[server]
host = "127.0.0.1"
port = 8777

[paths]
pipelines = "pipelines"

[runtime]
log_retention_runs = 50
console_buffer_lines = 2000
stop_grace_seconds = 20
control_poll_seconds = 0.5
# python_executable = "C:/path/to/python.exe"   # interpreter used for run processes
```

Environment overrides: `GRAETL_ROOT`, `GRAETL_PIPELINES_DIR`, `GRAETL_HOST`,
`GRAETL_PORT`.

---

## Connecting real sources

The demo pipelines use stdlib `sqlite3` and `csv` so they run with no extra
dependencies. Install the `data` extra for real work:

```bash
uv sync --extra data      # SQLAlchemy, pandas, openpyxl
```

```python
@pipeline.setup
def setup(ctx):
    from sqlalchemy import create_engine
    ctx.resources["src"] = create_engine(ctx.setting("source.dsn")).connect()
```

Keep credentials out of `pipeline.toml` — read them from the environment
(`os.environ["SRC_PASSWORD"]`) and reference them in the DSN.

---

## Tests

```bash
python -m unittest discover -s tests     # no extra dependencies
pytest                                   # if pytest is installed
```

The suite covers the incremental selection rules, transactional rollback,
dependency gating, the full pause/resume/stop cycle against a real runner
process, failure isolation, broken pipeline files and the WebSocket console.

---

## Project layout

```
graetl.toml            project configuration
pyproject.toml         package + dependencies (uv)
backend/graetl/
  config.py            settings + paths
  loader.py            pipeline + module-folder discovery, import, scaffolding
  cli.py               the `graetl` command
  sdk/                 what pipeline code imports (Pipeline, Entity, ctx)
  store/               core.py = etl.db, state.py = per-pipeline state.db
  runner/              the isolated run process: executor, control, events
  server/              Starlette API, supervisor, event hub
    static/            the console: plain ES modules, no build step
pipelines/             your pipelines (and the demos)
tests/                 end-to-end tests
```
