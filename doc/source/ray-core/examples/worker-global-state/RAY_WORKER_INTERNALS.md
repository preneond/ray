# Ray Worker Internals: Process Lifecycle, Job Isolation, venv Propagation, and Cluster Communication

This document explains how Ray creates and manages worker processes, how tasks are
assigned to workers, how runtime environments (venvs) are set up, how head and worker
nodes communicate, and how `ray job submit` and `ray.init()` (Ray Client) differ.

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

## 5. Summary: Isolation Guarantees

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

## 8. Head Node ↔ Worker Node Communication

### 8.1 Cluster topology

Every node (head and workers) runs the same pair of daemons:

```
┌─ Head Node ──────────────────────────────────────────────────────────┐
│  GCS Server          ← single cluster-wide authority for metadata    │
│  Raylet              ← local scheduler + worker pool                 │
│  RuntimeEnv Agent    ← local venv installer                          │
│  Dashboard / JobHead ← HTTP API gateway                              │
│  Object Store        ← local Plasma store                            │
└──────────────────────────────────────────────────────────────────────┘

┌─ Worker Node ────────────────────────────────────────────────────────┐
│  Raylet              ← local scheduler + worker pool                 │
│  RuntimeEnv Agent    ← local venv installer (independent of head)   │
│  Object Store        ← local Plasma store                            │
└──────────────────────────────────────────────────────────────────────┘
```

### 8.2 Worker node registration (join flow)

When a worker node boots its raylet calls `RegisterGcs()` (`node_manager.cc:305`).
This sends a `RegisterNode` RPC to GCS on the head node.

GCS handler (`gcs_node_manager.cc:102-156`):
1. Writes node metadata (address, port, resources, labels) to `NodeTable` in GCS storage.
2. Adds node to in-memory `alive_nodes_` map.
3. Broadcasts `PublishNodeInfoToPubsub()` so every other raylet learns about the new peer.

All subsequent raylets subscribe to this pubsub feed and update their local view of the
cluster without polling GCS again.

### 8.3 Resource reporting — RaySyncer

Each raylet periodically broadcasts its resource availability to all peers through the
**RaySyncer** subsystem (a lightweight gossip protocol over gRPC).

```
Worker node raylet
  │
  │  every report_resources_period_ms
  ▼
LocalResourceManager::CreateSyncMessage()   ← local_resource_manager.cc:423
  creates RESOURCE_VIEW message with:
    resources_total, resources_available, resource_load
  │
  ▼
RaySyncer sends to every connected peer (other raylets + GCS)
  │
  ├─► Head node GCS
  │     GcsNodeManager::UpdateAliveNode()  ← gcs_node_manager.cc:232
  │     Keeps per-node resource snapshot for autoscaler
  │
  └─► All other raylets
        Each raylet updates its local ClusterResourceManager view
        Used for scheduling decisions without needing to query GCS
```

This means **scheduling is fully decentralised** — each raylet makes placement decisions
using its local (eventually-consistent) copy of cluster resources.

### 8.4 Cross-node task scheduling

When a task cannot be satisfied locally (not enough resources), the local raylet spills it:

```
Driver submits task to local raylet via RequestWorkerLease RPC
  │
  ▼
node_manager.cc:HandleRequestWorkerLease()
  │
  ▼
ClusterLeaseManager::ScheduleAndGrantLeases()  ← cluster_lease_manager.cc:196
  calls GetBestSchedulableNode() to pick target
  │
  ├── target = local node  →  PopWorker() → assign to idle/new worker
  │
  └── target = remote node  →  AllocateRemoteTaskResources()
                                 then forwards RequestWorkerLease RPC
                                 directly to the remote node's raylet
                                 (node-to-node gRPC, not through head)
```

Key point: **inter-raylet task forwarding is peer-to-peer — it does not go through the
head node or GCS.** Head-node GCS is only consulted for initial node discovery and
autoscaler decisions.

### 8.5 Cross-node object transfer

When a task on node B needs an object that lives in node A's Plasma store:

```
Core worker on node B calls ray.get(ref)
  │
  ▼
ObjectManager::Pull()   ← pull_manager.cc:52
  queries GCS for object locations  (one-time lookup)
  │
  ▼
PullManager sends Pull RPC directly to node A's ObjectManager
  │
  ▼
Node A's ObjectManager pushes object bytes over gRPC to node B
  │
  ▼
Object lands in node B's Plasma store
Core worker on node B deserialises the result
```

Object transfers are **direct node-to-node**; head node is only involved in the
initial location lookup stored in GCS.

### 8.6 RuntimeEnv Agent — local per node

Each node runs **its own** RuntimeEnv Agent; there is no central venv server on the head.

```
Worker node raylet needs a venv for an incoming task
  │
  ▼
WorkerPool::StartNewWorker()
  calls runtime_env_agent_client_.GetOrCreateRuntimeEnv()
  via HTTP POST to localhost:<agent_port>/get_or_create_runtime_env
  ← runtime_env_agent_client.cc:367
  │
  ▼
Local RuntimeEnv Agent installs the venv on this node's disk
Returns RuntimeEnvContext JSON (py_executable path etc.)
  │
  ▼
Raylet spawns worker process with that context
```

If the same spec was already installed by a previous job on this node, the agent returns
the cached path immediately (no network involved).

Working-dir archives are fetched from the GCS internal KV store (where the submitter
uploaded them) — this is the one step that does go through the head node.

---

## 9. `ray job submit` — Code from Laptop to Cluster

### 9.1 Overview

```
Laptop (ray job submit)
  │  HTTP
  ▼
Head node Dashboard (port 8265)
  │  HTTP (internal)
  ▼
Job Agent (Dashboard plugin)
  │  Ray actor spawn
  ▼
JobSupervisor actor (on head node worker)
  │  subprocess.Popen
  ▼
Driver process running on head node
  │  CoreWorker gRPC
  ▼
Raylets on head + worker nodes (task execution)
```

### 9.2 Step-by-step

**1. CLI entry point** (`dashboard/modules/job/cli.py:218`)

`ray job submit` calls `JobSubmissionClient.submit_job()`.

**2. Working-dir upload** (`dashboard/modules/dashboard_sdk.py:420`)

Before the job is submitted, the client packages the local `working_dir` into a zip
archive and uploads it via:
```
PUT http://<head>:8265/api/packages/{protocol}/{hash}
```
The archive is stored in the GCS internal KV store under a content-addressed key so
all nodes can fetch it. (`job_head.py:373`, `dashboard_sdk.py:355`)

**3. HTTP job submission** (`sdk.py:262`)

```
POST http://<head>:8265/api/jobs/
Body: { entrypoint, runtime_env, metadata, ... }
```

**4. Head node routing** (`job_head.py:397-407`)

`JobHead` picks a job agent (Dashboard agent process) and forwards the request:
```
POST http://localhost:<agent_port>/api/job_agent/jobs/
```

**5. JobManager creates supervisor actor** (`job_manager.py:537-616`)

- Writes `JobInfo` with status `PENDING` to GCS internal KV store.
- Spawns a `JobSupervisor` Ray actor (this runs inside the cluster on the head node
  by default, since `num_cpus=0` lets it land without consuming CPU slots).

**6. Driver subprocess** (`job_supervisor.py:155-200`)

`JobSupervisor.run()` calls `subprocess.Popen(entrypoint, shell=True, ...)`.
The driver process is a plain OS process on the head node.
Key env vars set for it:
- `RAY_ADDRESS` — so `ray.init()` inside the script auto-connects
- `RAY_JOB_CONFIG_JSON_ENV_VAR` — carries the runtime_env and metadata

**7. Status tracking**

`JobSupervisor` polls the subprocess return code and writes status transitions
(`PENDING → RUNNING → SUCCEEDED/FAILED`) back to GCS KV (`common.py:278`).

The submitter polls `GET /api/jobs/{job_id}` which reads from the same GCS KV.

### 9.3 Where things run

| Component | Runs on |
|---|---|
| `ray job submit` CLI | Laptop (outside cluster) |
| JobHead / Dashboard | Head node |
| JobSupervisor actor | Head node (num_cpus=0 actor) |
| Driver process (entrypoint) | Head node subprocess |
| Tasks and actors from the driver | Any node (scheduled by raylets) |

---

## 10. `ray.init()` — Attached / Ray Client Mode

### 10.1 Two sub-modes of ray.init()

| Invocation | Mode | Driver location |
|---|---|---|
| `ray.init()` (no address) | **Local** — starts a mini cluster in-process | Same machine |
| `ray.init(address="auto")` | **Direct** — connects as native driver to existing cluster | Same machine as head |
| `ray.init(address="ray://host:10001")` | **Ray Client** — connects over gRPC from anywhere | Laptop / CI / anywhere |

### 10.2 ray.init() — local mode

`worker.py:1947` spawns a `Node(head=True)` which starts all Ray system processes
(GCS, Raylet, Plasma, Dashboard) as subprocesses. The driver then connects to this
mini cluster as a native CoreWorker (`worker.py:2709`).

### 10.3 ray.init() — direct mode (address="auto" or IP)

The driver connects its C++ CoreWorker directly to the cluster's GCS and raylet.
No gRPC proxy layer — the driver registers a job and submits tasks with the same
low-latency path as a job-submit driver. This mode requires the driver to be on a
machine that has network access to internal cluster ports (raylet ports, GCS port).

### 10.4 ray.init() — Ray Client mode (address="ray://...")

```
Laptop
  ray.init("ray://head:10001")
  │
  │  gRPC (port 10001, bidirectional stream)
  ▼
Head node — Ray Client Server (ray/util/client/server/server.py)
  │
  │  Native in-process CoreWorker calls
  ▼
GCS + Raylets (task scheduling, object store)
```

**Code path** (`worker.py:1711`):
```python
# worker.py:1711-1713
if address is not None and "://" in address:
    builder = ray.client(address, ...)
    return builder.connect()
```

`ClientBuilder.connect()` (`client_builder.py:136`) establishes a gRPC channel to the
Ray Client Server and calls `Init` RPC to negotiate the session.

**The `RayletDriver` gRPC service** (`src/ray/protobuf/ray_client.proto:324`) exposes:

| RPC | Purpose |
|---|---|
| `Init` | Establish session, send JobConfig |
| `Schedule(ClientTask)` | Submit a remote() call; returns ObjectRef ID |
| `GetObject(ref)` | Fetch object value from cluster object store |
| `PutObject(value)` | Upload object to cluster object store |
| `WaitObject` | ray.wait() |
| `KVGet/Put/Del/List` | Ray internal KV operations |
| `Datapath` (streaming) | Bidirectional stream multiplexing all data ops |

**How `my_task.remote()` works in client mode** (`client/worker.py:631`):

1. Client serialises function + args into a `ClientTask` proto.
2. Sends it via `data_client.Schedule(task)` over the gRPC `Datapath` stream.
3. Server-side (`server.py:608`) deserialises and calls the real `remote_func.remote()`
   **on the cluster** — the task is dispatched into Ray's native scheduling path.
4. Server returns the ObjectRef ID to the client.
5. Client calls `GetObject(ref)` over gRPC only when `ray.get()` is called — not eagerly.

### 10.5 Key differences: Ray Client vs job submit

| Aspect | `ray job submit` | `ray.init("ray://...")` Ray Client |
|---|---|---|
| **Driver location** | Head node subprocess | Laptop / external machine |
| **Connection protocol** | No proxy; driver is native CoreWorker on head | gRPC bidirectional stream through Ray Client Server |
| **Task submission path** | Direct CoreWorker → Raylet | Client → gRPC → Client Server → CoreWorker → Raylet |
| **Latency per task** | ~microseconds (local IPC) | ~milliseconds (gRPC round-trip per Schedule call) |
| **Serialisation boundary** | None — driver and workers share same cluster | All args/results serialised over gRPC |
| **Network drop** | Job keeps running; only affects log streaming | Session may be lost; reconnect attempted within `reconnect_grace_period` |
| **Job lifetime** | Independent of submitter; survives terminal close | Tied to gRPC session (configurable grace period) |
| **Large object args** | Stored in local Plasma, passed by ref | Must be `ray.put()` first; otherwise serialised through gRPC |
| **Working dir** | Uploaded as zip before submission | Specified in runtime_env; uploaded same way |
| **Recommended for** | Production, CI, batch jobs | Interactive notebooks, debugging, REPL |

### 10.6 Driver registration with GCS

Both modes end up registering a job with GCS. For a job-submit driver:

```python
# worker.py:2573  (inside connect())
job_id = ray._private.state.next_job_id()   # GCS call to allocate ID

# worker.py:2709
CoreWorker(mode=SCRIPT_MODE, job_id=job_id, ...)
# CoreWorker C++ constructor sends RegisterJob RPC to GCS
# GCS stores JobTableData: job_id, driver_ip, driver_pid, start_time, config
```

For Ray Client, the registration happens on the **server side** when the `Init` RPC
is processed (`server.py:146`) — the client itself never touches GCS directly.

---

## 11. Key Source Files (updated)

| File | What it does |
|---|---|
| `src/ray/raylet/worker_pool.cc` | Worker pool: idle reuse, job matching, kill-on-finish |
| `src/ray/raylet/worker.cc` | Per-worker state; `SetJobId()` one-time binding |
| `src/ray/core_worker/context.cc` | `MaybeInitializeJobInfo()` — job binding inside worker |
| `src/ray/raylet/node_manager.cc` | Task lease handling, resource reporting, node registration |
| `src/ray/raylet/scheduling/cluster_lease_manager.cc` | Cross-node task scheduling logic |
| `src/ray/gcs/gcs_node_manager.cc` | Node registration and pubsub on head |
| `src/ray/object_manager/pull_manager.cc` | Cross-node object pull protocol |
| `python/ray/_private/workers/default_worker.py` | Worker entry point; deserialises RuntimeEnvContext |
| `python/ray/_private/runtime_env/agent/runtime_env_agent.py` | venv install orchestration, caching, locking |
| `python/ray/_private/runtime_env/pip.py` | pip install logic, hash computation, context modification |
| `python/ray/_private/runtime_env/uv.py` | uv install logic (same structure as pip.py) |
| `python/ray/_private/runtime_env/context.py` | `RuntimeEnvContext` — carries py_executable + env_vars to worker |
| `python/ray/_private/runtime_env/uri_cache.py` | Reference-counted venv cache with size-based eviction |
| `python/ray/dashboard/modules/job/job_manager.py` | JobManager: supervisor actor creation, status tracking |
| `python/ray/dashboard/modules/job/job_supervisor.py` | Driver subprocess spawn, log streaming |
| `python/ray/dashboard/modules/job/sdk.py` | `JobSubmissionClient` — HTTP client for job API |
| `python/ray/dashboard/modules/job/job_head.py` | Dashboard HTTP endpoints for job submission |
| `python/ray/util/client/server/server.py` | Ray Client Server: `Schedule`, `Init`, `GetObject` RPCs |
| `python/ray/util/client/worker.py` | Ray Client: sends `Schedule` RPC for each `remote()` call |
| `src/ray/protobuf/ray_client.proto` | `RayletDriver` gRPC service definition |
| `python/ray/_private/worker.py` | `ray.init()` — routing between local/direct/client modes |
