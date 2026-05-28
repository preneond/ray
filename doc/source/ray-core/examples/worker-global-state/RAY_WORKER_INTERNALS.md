# Ray Worker Internals: Process Lifecycle, Job Isolation, and venv Propagation

This document explains how Ray creates and manages worker processes, how tasks are
assigned to workers, how runtime environments (venvs) are set up, and what the actual
isolation guarantees are between jobs.

All source references are relative to the Ray repository root.

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Ray Cluster                                                     │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Head Node                                               │    │
│  │                                                          │    │
│  │  ┌──────────┐  ┌──────────────┐  ┌──────────────────┐  │    │
│  │  │  Driver  │  │  GCS Server  │  │ RuntimeEnv Agent │  │    │
│  │  │ (user    │  │ (global      │  │ (venv installer) │  │    │
│  │  │  script) │  │  control)    │  └──────────────────┘  │    │
│  │  └──────────┘  └──────────────┘                         │    │
│  │  ┌──────────────────────────────────────────────────┐   │    │
│  │  │  Raylet (worker pool + scheduler)                │   │    │
│  │  │  ┌──────────┐ ┌──────────┐ ┌──────────┐         │   │    │
│  │  │  │ Worker 0 │ │ Worker 1 │ │ Worker 2 │  ...    │   │    │
│  │  │  │ job_id=A │ │ job_id=A │ │  (idle)  │         │   │    │
│  │  │  └──────────┘ └──────────┘ └──────────┘         │   │    │
│  │  └──────────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Worker Node (same structure, no GCS/Driver)             │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

Key processes per node:
- **Raylet** — C++ daemon; owns the worker pool and local scheduler
- **RuntimeEnv Agent** — Python daemon; installs venvs, manages env lifecycle
- **Worker processes** — Python processes; execute tasks and actor methods

---

## 2. How a Task Gets Executed: End-to-End Flow

```
Driver calls my_task.remote(arg)
          │
          ▼
 1. Core worker (driver-side) serializes task spec + args
    and submits it to the local raylet via gRPC
          │
          ▼
 2. Raylet scheduler decides which node should run the task
    (based on resources, locality, runtime_env affinity)
          │
          ▼
 3. Raylet on the target node calls WorkerPool::PopWorker()
          │
          ├─── idle worker with matching job_id + runtime_env_hash?
          │         YES → lease that worker
          │
          └─── NO → call StartNewWorker()
                      │
                      ├─ if runtime_env is non-empty:
                      │     ask RuntimeEnv Agent to create/get venv
                      │     wait for agent reply (env URI + context JSON)
                      │
                      └─ spawn new Python worker process
                         with env vars: RAY_JOB_ID, ray_runtime_env_hash,
                         --serialized-runtime-env-context=<json>
          │
          ▼
 4. Worker process starts (default_worker.py)
    - deserializes RuntimeEnvContext from env var
    - activates venv (prepends venv/bin to PATH, sets sys.prefix)
    - runs setup_hook if specified
    - registers with raylet
          │
          ▼
 5. Raylet assigns the task to the worker via AssignTask RPC
          │
          ▼
 6. Worker's core worker receives task, calls execute_task()
    - imports the remote function module (cached in sys.modules)
    - deserializes arguments from object store
    - calls the Python function
    - puts return value into object store
          │
          ▼
 7. Worker reports completion to raylet, returns to idle pool
          │
          ▼
 8. Driver's ray.get() receives the object ref and fetches the result
```

Source: `src/ray/raylet/worker_pool.cc`, `python/ray/_private/workers/default_worker.py`,
`src/ray/core_worker/core_worker.cc`

---

## 3. Worker Pool: Reuse and Job Binding

### 3.1 Worker is bound to one job for life

When a worker process first receives a task, the raylet calls `Worker::SetJobId()`:

```cpp
// src/ray/raylet/worker.cc
void Worker::SetJobId(const JobID &job_id) {
  if (assigned_job_id_.IsNil()) {
    assigned_job_id_ = job_id;   // set once, never changed
  }
  RAY_CHECK(assigned_job_id_ == job_id);  // crash if different job tries to use this worker
}
```

The same guard exists inside the worker process itself (`context.cc::MaybeInitializeJobInfo`):

```cpp
if (!current_job_id_.IsNil() && job_config_.has_value()) {
  RAY_CHECK(current_job_id_ == job_id);  // hard crash on job_id mismatch
  return;
}
```

**Consequence: a worker process can never execute tasks from two different jobs.**
Cross-job global state contamination (e.g. a cached object from Job A leaking into Job B) is
structurally impossible.

### 3.2 Idle worker selection criteria

`WorkerPool::FindAndPopIdleWorker()` (`worker_pool.cc`) checks every idle worker against:

| Check | Condition to pass |
|---|---|
| Language | must match (Python / Java / C++) |
| Worker type | must match (WORKER / IO_WORKER) |
| **Job ID** | must be Nil (unassigned) **or** equal to the task's job_id |
| runtime_env_hash | must be identical |
| dynamic_options | must be identical |
| Exiting | worker must not be in pending-exit state |

If no idle worker passes all checks, `StartNewWorker()` is called.

### 3.3 What happens when a job finishes

`WorkerPool::HandleJobFinished()` adds the job_id to `finished_jobs_`.
The next time one of that job's workers becomes idle, the kill-idle-workers timer picks it up:

```cpp
// worker_pool.cc
if (finished_jobs_.contains(job_id) && idle_worker->GetRootDetachedActorId().IsNil()) {
    request.set_force_exit(true);   // worker receives Exit RPC and terminates
}
```

Workers are killed, not recycled to other jobs.

### 3.4 Within-job reuse: global state IS shared

Because the same worker process handles multiple sequential tasks from the same job,
all module-level Python state persists across task invocations:

- Module imports (`sys.modules`) — once imported, stay imported
- `functools.lru_cache` decorated functions — cache entries accumulate
- Module-level variables — mutations from task N are visible to task N+1
- Class variables — shared across all tasks in the same worker

This is demonstrated concretely in `worker_global_state_demo.py` in this directory.

**Workaround**: set `max_calls=N` on `@ray.remote` to force worker exit after N invocations:

```python
@ray.remote(max_calls=1)  # fresh process per task; clean global state
def my_task(): ...
```

---

## 4. Runtime Environment Setup (venv with uv/pip)

### 4.1 What a runtime_env looks like

```python
ray.init(runtime_env={
    "pip": ["numpy==1.26", "pandas==2.2"],   # pip list  (or use "uv")
    "uv": ["numpy==1.26", "pandas==2.2"],    # uv list
    "working_dir": "./src",                   # uploaded to all nodes
    "env_vars": {"MY_VAR": "value"},
})
```

### 4.2 Hashing: same spec = same venv

`pip.py::_get_pip_hash()` / `uv.py` compute a **SHA-1 hash** over the sorted,
serialised dependency specification.  This hash becomes the venv's disk path and
the worker's `runtime_env_hash` identity:

```
hash("numpy==1.26,pandas==2.2") → abc123
venv path: /tmp/ray/session_<ts>/runtime_resources/uv/abc123/virtualenv/
```

Two jobs with identical deps share the same on-disk venv but still get separate worker
processes (different job_ids → different workers).

### 4.3 Installation flow

```
Task arrives at raylet with non-empty runtime_env
          │
          ▼
WorkerPool::StartNewWorker()
  calls GetOrCreateRuntimeEnv(serialized_runtime_env, job_id, callback)
          │
          ▼
RuntimeEnv Agent (runtime_env_agent.py::GetOrCreateRuntimeEnv RPC)
  1. Check _env_cache[serialized_env] → already built? return cached result
  2. Acquire asyncio.Lock for this serialized_env  (prevents parallel installs)
  3. Run plugins in priority order:
       working_dir plugin → downloads working_dir archive, extracts it
       pip / uv plugin    → creates or reuses venv, installs packages
       env_vars plugin    → records extra env vars for worker
  4. Return RuntimeEnvContext JSON (py_executable path, command_prefix, env_vars)
  5. Store result in _env_cache
          │
          ▼
Worker process is spawned with:
  --serialized-runtime-env-context=<JSON>
  WORKER_PROCESS_SETUP_HOOK_ENV_VAR set if setup_hook defined
          │
          ▼
default_worker.py startup:
  context = RuntimeEnvContext.deserialize(json_str)
  context.exec_worker(...)  # replaces process image with venv's python,
                             # having run venv activation commands first
```

### 4.4 Concurrency safety

Two concurrent jobs with the **same** spec trigger only **one** install:

```python
# runtime_env_agent.py
if serialized_env not in self._env_locks:
    self._env_locks[serialized_env] = asyncio.Lock()

async with self._env_locks[serialized_env]:   # only one install at a time
    if serialized_env in self._env_cache:
        return self._env_cache[serialized_env]  # second job reuses result
    # ... actually install ...
```

At the pip/uv plugin level there is a second lock per URI (`_create_locks[uri]`) to
guard the venv directory itself.

### 4.5 venv activation inside the worker

`pip.py::modify_context()` sets two fields on the `RuntimeEnvContext`:

```python
context.py_executable = "<venv>/bin/python"
context.command_prefix += ["source", "<venv>/bin/activate", "&&"]
```

`context.exec_worker()` execs the worker command with these prefixes, so the worker
process runs entirely inside the venv from birth.  There is no dynamic activation after
startup — the venv is baked into the process image.

### 4.6 Per-node caching and GC

Each node caches venvs independently under:
```
/tmp/ray/session_<timestamp>/runtime_resources/
  pip/<hash>/virtualenv/
  uv/<hash>/virtualenv/
  working_dir_files/<hash>/
```

The `URICache` tracks reference counts per URI.  When a job finishes,
`HandleJobFinished` calls `DeleteRuntimeEnvIfPossible`.  If the reference count drops
to zero, the venv directory is eligible for deletion.  Cache size is bounded by
`RAY_RUNTIME_ENV_<field>_CACHE_SIZE_GB` (default 10 GB per field).

---

## 5. Head Node vs Worker Node Differences

| Component | Head node | Worker node |
|---|---|---|
| GCS Server | ✅ runs here | ❌ |
| Driver process | ✅ by default | ❌ |
| RuntimeEnv Agent | ✅ | ✅ (one per node) |
| Raylet | ✅ | ✅ |
| Worker processes | ✅ | ✅ |
| Object Store (Plasma) | ✅ | ✅ |

The worker pool logic (including job binding, idle worker selection, and kill-on-finish)
runs identically on every node.  The head node is not special from a worker-lifecycle
perspective.

---

## 6. Summary: Isolation Guarantees

| Scenario | Isolated? | Explanation |
|---|---|---|
| Task A (Job 1) → Task B (Job 2), same node | **YES** | Workers are job-bound; `RAY_CHECK` crashes on mismatch |
| Task A (Job 1) → Task B (Job 1), same worker | **NO** | Same process; module-level state persists |
| `functools.lru_cache` across jobs | **YES** (safe) | Jobs never share a worker process |
| `functools.lru_cache` across tasks, same job | **NO** (shared) | Same process reused for multiple tasks |
| venv installation race between jobs | **YES** (safe) | asyncio lock serialises installs; result is cached |
| venv directory shared between jobs (same spec) | **YES** (safe) | Read-only after install; both jobs see consistent state |
| Installed package from Job A affecting Job B (different spec) | **YES** (safe) | Different hash → different directory |

---

## 7. Key Source Files

| File | What it does |
|---|---|
| `src/ray/raylet/worker_pool.cc` | Worker pool: idle reuse, job matching, kill-on-finish |
| `src/ray/raylet/worker.cc` | Per-worker state; `SetJobId()` one-time binding |
| `src/ray/core_worker/context.cc` | `MaybeInitializeJobInfo()` — job binding inside worker |
| `python/ray/_private/workers/default_worker.py` | Worker entry point; deserialises RuntimeEnvContext |
| `python/ray/_private/runtime_env/agent/runtime_env_agent.py` | venv install orchestration, caching, locking |
| `python/ray/_private/runtime_env/pip.py` | pip install logic, hash computation, context modification |
| `python/ray/_private/runtime_env/uv.py` | uv install logic (same structure as pip.py) |
| `python/ray/_private/runtime_env/context.py` | `RuntimeEnvContext` — carries py_executable + env_vars to worker |
| `python/ray/_private/runtime_env/uri_cache.py` | Reference-counted venv cache with size-based eviction |
