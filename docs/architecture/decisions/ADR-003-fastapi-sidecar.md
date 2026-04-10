# ADR-003: FastAPI Sidecar for Pipeline Execution

## Status
Accepted

## Context

The data pipeline is triggered by the Node.js API when an analyst uploads source files via the Data Ingestion page. The original trigger mechanism was a direct child process: the Node.js API called `spawn('python', ['main_silver_orchestrator.py'])` and waited for it to exit.

This approach had a critical latency problem. Python's import system loads every dependency at interpreter startup — pandas, SQLAlchemy, numpy, PyO3 bindings, and the full pipeline module tree. On the production machine this cold-start overhead measured approximately **2 minutes 20 seconds** before the orchestrator executed a single line of business logic. The total pipeline time was therefore ~3 minutes, of which ~80% was startup cost paid on every run.

A secondary problem was observability. A child process launched by Node.js produces stdout/stderr that the API has to capture and forward. There was no structured way to stream live log output to the frontend while the pipeline was running, and no way to query pipeline status without polling a file or a database row.

## Decision

A persistent FastAPI sidecar process (`pipeline_server.py`) runs alongside the Node.js API. It keeps a Python interpreter alive at all times with all imports pre-loaded. Node.js communicates with it over HTTP on port 8000 rather than spawning a new process.

The sidecar exposes four endpoints:

| Endpoint | Auth | Purpose |
|----------|------|---------|
| `GET /health` | None | Liveness check — confirms the server is alive and reports Python version |
| `POST /run` | `X-Pipeline-Key` | Launches the orchestrator in a background thread; returns 409 if already running |
| `GET /status` | None | Returns current state: `idle / running / completed / failed`, timestamps, exit code, last 1000 log lines |
| `GET /logs/stream` | `X-Pipeline-Key` | Server-Sent Events stream — pushes each new log line to the client in real time (300 ms poll interval) |

When `POST /run` is received, the server launches `main_silver_orchestrator.py` as a subprocess using `subprocess.Popen` with the same Python executable that started the server. Stdout and stderr are captured line-by-line in two daemon threads and appended to an in-memory log buffer (capped at 1000 lines). A `threading.Lock` ensures the shared state dictionary is safe across the server thread, the two reader threads, and any concurrent HTTP request handlers.

Only one pipeline execution runs at a time. A second `POST /run` while a run is in progress returns immediately with `started: false` and the current `job_id`.

Authentication uses a shared secret (`PIPELINE_API_KEY`) passed as an `X-Pipeline-Key` request header. `/run` and `/logs/stream` require the key; `/health` and `/status` are unauthenticated. The Node.js API sets the key in its `.env` and passes it on every request to the sidecar.

The sidecar is started separately from the Node.js API — either via `python pipeline_server.py` or the provided `start_pipeline_server.bat` on Windows. It is not a child of Node.js and does not restart when Node.js restarts.

## Consequences

### Positive

- **Cold-start cost is eliminated.** Python and all imports load once at server startup. Subsequent `POST /run` calls pay only the actual pipeline execution time (~44 seconds), not the import overhead (~2m 20s).
- **Live log streaming is possible.** The SSE endpoint at `/logs/stream` lets the frontend display each pipeline log line as it is produced, without polling and without WebSockets. The Node.js API proxies this stream to the browser.
- **Pipeline status is queryable.** `GET /status` returns structured JSON with timestamps, exit code, and the running log. The frontend uses this to show a progress indicator and detect completion.
- **Single-execution enforcement is built in.** The in-memory lock prevents concurrent runs without any database coordination. A second trigger from the UI is rejected immediately with a clear message.
- **The sidecar is language-decoupled.** Node.js sends HTTP requests; it has no knowledge of Python imports, SQLAlchemy sessions, or pipeline internals. If the orchestrator is rewritten, the HTTP contract stays the same.

### Negative / Trade-offs

- **Two processes to manage.** The pipeline server must be started independently and kept alive. If it crashes, `POST /run` from Node.js returns a connection error. There is no supervisor (systemd unit, PM2 config, or Docker restart policy) defined in the repository.
- **State is in memory only.** The log buffer and run state are lost if the sidecar restarts. A restart mid-run leaves Node.js with no way to query what happened to the previous job.
- **The subprocess approach negates some of the warm-process benefit.** `POST /run` launches `main_silver_orchestrator.py` as a new subprocess, so that subprocess still pays its own cold-start cost. The sidecar eliminates the Node.js→Python spawn cost but not the orchestrator's own import time. The 44-second runtime reflects the orchestrator's subprocess startup plus actual processing.
- **No process isolation between runs.** Because the sidecar is a single persistent process, a memory leak or corrupted global state in one pipeline run could affect subsequent runs. In practice this has not been observed, but it is a structural risk of the warm-process model.
- **Authentication is a shared secret.** `X-Pipeline-Key` is a static key set in `.env`. There is no key rotation mechanism, no expiry, and no per-client identity. This is acceptable for an internal network service but would not be appropriate for an externally exposed endpoint.
