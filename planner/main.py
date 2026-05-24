from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, parse
from urllib import request as urlrequest
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("helios-planner")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_dir = Path(os.getenv("HELIOS_LOG_DIR", "logs"))
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "planner.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError as exc:
        logger.warning("file logging disabled: %s", exc)
    return logger


LOGGER = configure_logging()

PLANNER_BACKEND = os.getenv("HELIOS_PLANNER_BACKEND", "gemini").strip().lower()
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or os.getenv("HELIOS_PLANNER_API_KEY") or "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
GEMINI_API_BASE = os.getenv(
    "GEMINI_API_BASE", "https://generativelanguage.googleapis.com/v1beta"
).rstrip("/")
GEMINI_TIMEOUT_SECONDS = float(os.getenv("GEMINI_TIMEOUT_SECONDS", "20"))
RUNBOOK_DIR = Path(os.getenv("HELIOS_RUNBOOK_DIR", Path(__file__).with_name("runbooks")))
HELIOS_TIMEZONE = (
    os.getenv("HELIOS_TIMEZONE", os.getenv("TZ", "Asia/Kolkata")).strip() or "Asia/Kolkata"
)
LOCAL_TIMEZONE = ZoneInfo(HELIOS_TIMEZONE)
SUPPORTED_TASK_TYPES = [
    "validate_payload",
    "transform_records",
    "model_inference",
    "aggregate_metrics",
    "write_artifact",
    "notify_webhook",
]

app = FastAPI(title="Helios Planner", version="1.2.0")

PLANNER_REQUESTS = Counter(
    "helios_planner_requests_total",
    "Planner API requests by operation, backend, and status.",
    ["operation", "backend", "status"],
)
GENAI_REQUESTS = Counter(
    "helios_genai_requests_total",
    "GenAI provider requests by operation, model, and status.",
    ["operation", "model", "status"],
)
GENAI_TOKEN_ESTIMATE = Counter(
    "helios_genai_token_estimate_total",
    "Estimated GenAI token usage by operation, model, and direction.",
    ["operation", "model", "direction"],
)
GENAI_LATENCY_SECONDS = Histogram(
    "helios_genai_latency_seconds",
    "GenAI provider request latency.",
    ["operation", "model"],
    buckets=(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30),
)
RAG_RETRIEVALS = Counter(
    "helios_rag_retrievals_total",
    "Runbook retrieval requests by status.",
    ["status"],
)


class RetryPolicy(BaseModel):
    max_attempts: int = 3
    initial_backoff_seconds: int = 2
    max_backoff_seconds: int = 30
    multiplier: float = 2.0


class IntentRequest(BaseModel):
    name: str = Field(..., description="Workflow name")
    intent: str = Field(..., description="Natural language workload description")
    stages: list[str] = Field(default_factory=list)
    timeout_seconds: int = 30
    retry_policy: RetryPolicy = RetryPolicy()


class WorkflowTask(BaseModel):
    task_id: str
    task_type: str
    dependencies: list[str] = Field(default_factory=list)
    input_payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int
    retry_policy: RetryPolicy
    metadata: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str | None = None


class WorkflowSpec(BaseModel):
    name: str
    metadata: dict[str, str] = Field(default_factory=dict)
    tasks: list[WorkflowTask]


class IntentResponse(BaseModel):
    workflow: WorkflowSpec
    planning_notes: list[str]
    scheduler_hints: dict[str, Any]
    created_at: datetime


class ThroughputSignal(BaseModel):
    task_type: str
    p95_ms: int
    retry_rate: float


class OptimizationRequest(BaseModel):
    workflow_name: str
    current_concurrency: int
    signals: list[ThroughputSignal] = Field(default_factory=list)


class OptimizationResponse(BaseModel):
    recommended_concurrency: int
    batching_recommendation: str
    stragglers: list[str]
    notes: list[str]


class RunbookMatch(BaseModel):
    title: str
    path: str
    score: float
    excerpt: str


class FailureAnalysisRequest(BaseModel):
    workflow: dict[str, Any]
    runbook_query: str | None = None
    include_raw_snapshot: bool = False


class FailureAnalysisResponse(BaseModel):
    summary: str
    failure_class: str
    likely_root_causes: list[str]
    recovery_actions: list[str]
    affected_tasks: list[str]
    runbook_matches: list[RunbookMatch]
    confidence: float
    backend: str
    generated_at: datetime
    raw_snapshot: dict[str, Any] | None = None


class PlannedTaskDraft(BaseModel):
    task_id: str
    task_type: str
    dependencies: list[str] = Field(default_factory=list)
    input_payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int | None = None
    metadata: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str | None = None


class GeminiIntentDraft(BaseModel):
    tasks: list[PlannedTaskDraft]
    planning_notes: list[str] = Field(default_factory=list)
    scheduler_hints: dict[str, Any] = Field(default_factory=dict)


class GeminiOptimizationDraft(BaseModel):
    recommended_concurrency: int
    batching_recommendation: str
    stragglers: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class GeminiFailureDraft(BaseModel):
    summary: str
    failure_class: str
    likely_root_causes: list[str] = Field(default_factory=list)
    recovery_actions: list[str] = Field(default_factory=list)
    affected_tasks: list[str] = Field(default_factory=list)
    confidence: float = 0.65


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {
        "status": "ok",
        "planner_backend": PLANNER_BACKEND,
        "active_backend": select_backend(),
        "gemini_configured": bool(GEMINI_API_KEY),
        "gemini_model": GEMINI_MODEL if GEMINI_API_KEY else "",
        "runbook_dir": str(RUNBOOK_DIR),
        "timezone": HELIOS_TIMEZONE,
    }


@app.get("/metrics")
def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/v1/plan/intent", response_model=IntentResponse)
def plan_intent(request: IntentRequest) -> IntentResponse:
    backend = select_backend()
    if backend == "gemini":
        response = plan_with_gemini(request)
        if response is not None:
            PLANNER_REQUESTS.labels("intent", "gemini", "ok").inc()
            return response
        if is_strict_gemini():
            PLANNER_REQUESTS.labels("intent", "gemini", "error").inc()
            raise HTTPException(
                status_code=503,
                detail=(
                    "Gemini planner is required but unavailable. Set GEMINI_API_KEY "
                    "and verify Gemini API access."
                ),
            )
        PLANNER_REQUESTS.labels("intent", "gemini", "fallback").inc()
    else:
        PLANNER_REQUESTS.labels("intent", "heuristic", "ok").inc()
    return heuristic_plan_intent(
        request,
        extra_notes=fallback_notes("intent planning"),
    )


@app.post("/v1/optimize", response_model=OptimizationResponse)
def optimize(request: OptimizationRequest) -> OptimizationResponse:
    backend = select_backend()
    if backend == "gemini":
        response = optimize_with_gemini(request)
        if response is not None:
            PLANNER_REQUESTS.labels("optimize", "gemini", "ok").inc()
            return response
        if is_strict_gemini():
            PLANNER_REQUESTS.labels("optimize", "gemini", "error").inc()
            raise HTTPException(
                status_code=503,
                detail=(
                    "Gemini optimizer is required but unavailable. Set GEMINI_API_KEY "
                    "and verify Gemini API access."
                ),
            )
        PLANNER_REQUESTS.labels("optimize", "gemini", "fallback").inc()
    else:
        PLANNER_REQUESTS.labels("optimize", "heuristic", "ok").inc()
    return heuristic_optimize(
        request,
        extra_notes=fallback_notes("throughput optimization"),
    )


@app.post("/v1/analyze/failure", response_model=FailureAnalysisResponse)
def analyze_failure(request: FailureAnalysisRequest) -> FailureAnalysisResponse:
    runbook_matches = retrieve_runbooks(build_failure_query(request))
    backend = select_backend()
    if backend == "gemini":
        response = analyze_failure_with_gemini(request, runbook_matches)
        if response is not None:
            PLANNER_REQUESTS.labels("failure_analysis", "gemini", "ok").inc()
            return response
        if is_strict_gemini():
            PLANNER_REQUESTS.labels("failure_analysis", "gemini", "error").inc()
            raise HTTPException(
                status_code=503,
                detail=(
                    "Gemini failure analyzer is required but unavailable. Set "
                    "GEMINI_API_KEY and verify Gemini API access."
                ),
            )
        PLANNER_REQUESTS.labels("failure_analysis", "gemini", "fallback").inc()
    else:
        PLANNER_REQUESTS.labels("failure_analysis", "heuristic", "ok").inc()
    return heuristic_failure_analysis(request, runbook_matches)


def select_backend() -> str:
    if PLANNER_BACKEND == "gemini":
        return "gemini"
    if PLANNER_BACKEND == "auto" and GEMINI_API_KEY:
        return "gemini"
    return "heuristic"


def is_strict_gemini() -> bool:
    return PLANNER_BACKEND == "gemini"


def heuristic_plan_intent(
    request: IntentRequest, extra_notes: list[str] | None = None
) -> IntentResponse:
    stages = request.stages or derive_stages(request.intent)
    tasks: list[WorkflowTask] = []
    previous_task_id = ""
    planning_notes = [
        "Planner uses deterministic heuristics in v1 so DAG generation stays inspectable.",
        (
            "Independent extract/validate/score style stages can be parallelized "
            "by splitting the input payload later."
        ),
        "Concurrency and batching should stay outside the correctness path of the control plane.",
    ]
    if extra_notes:
        planning_notes.extend(extra_notes)

    for index, stage in enumerate(stages):
        task_type = map_stage_to_task_type(stage)
        task_id = stage_slug(stage, index)
        dependencies = [previous_task_id] if previous_task_id else []
        payload = default_payload_for_task_type(task_type, stage)
        tasks.append(
            WorkflowTask(
                task_id=task_id,
                task_type=task_type,
                dependencies=dependencies,
                input_payload=payload,
                timeout_seconds=request.timeout_seconds,
                retry_policy=request.retry_policy,
                idempotency_key=default_idempotency_key(task_type, task_id),
                metadata={
                    "planner_origin": "heuristic-v1",
                    "intent_stage": stage,
                    "planner_backend": "heuristic",
                },
            )
        )
        previous_task_id = task_id

    workflow = WorkflowSpec(
        name=request.name,
        metadata={
            "planner": "helios-planner",
            "intent": request.intent,
            "planner_backend": "heuristic",
        },
        tasks=tasks,
    )
    return IntentResponse(
        workflow=workflow,
        planning_notes=planning_notes,
        scheduler_hints={
            "recommended_parallelism": max(1, min(4, len(stages) // 2 or 1)),
            "batch_size": 32 if "batch" in request.intent.lower() else 8,
            "backpressure_mode": "protect_workers",
        },
        created_at=now_local(),
    )


def heuristic_optimize(
    request: OptimizationRequest, extra_notes: list[str] | None = None
) -> OptimizationResponse:
    stragglers = [
        signal.task_type
        for signal in request.signals
        if signal.p95_ms > 5_000 or signal.retry_rate > 0.15
    ]
    recommended = request.current_concurrency
    notes = [
        "Planner recommendations are heuristics and should be operator-reviewed before rollout."
    ]
    if stragglers:
        recommended = max(1, request.current_concurrency - 1)
        notes.append(
            "Reduced concurrency because one or more task types show elevated "
            "latency or retry rate."
        )
    else:
        recommended = min(request.current_concurrency + 1, 8)
        notes.append("Slightly increased concurrency because recent signals are stable.")
    if extra_notes:
        notes.extend(extra_notes)
    batching = (
        "Use smaller batches for straggler task types."
        if stragglers
        else "Current batch size is healthy."
    )
    return OptimizationResponse(
        recommended_concurrency=recommended,
        batching_recommendation=batching,
        stragglers=stragglers,
        notes=notes,
    )


def plan_with_gemini(request: IntentRequest) -> IntentResponse | None:
    prompt = build_intent_prompt(request)
    try:
        payload = call_gemini_json("intent", prompt, intent_response_schema())
        draft = GeminiIntentDraft.model_validate(payload)
    except Exception as exc:
        LOGGER.warning("gemini intent planning failed: %s", exc)
        return None

    tasks: list[WorkflowTask] = []
    task_id_map = {
        draft_task.task_id: stage_slug(draft_task.task_id, index)
        for index, draft_task in enumerate(draft.tasks)
    }
    for draft_task in draft.tasks:
        task_type = (
            draft_task.task_type
            if draft_task.task_type in SUPPORTED_TASK_TYPES
            else map_stage_to_task_type(draft_task.task_id)
        )
        stage_name = draft_task.metadata.get("intent_stage", draft_task.task_id)
        tasks.append(
            WorkflowTask(
                task_id=task_id_map[draft_task.task_id],
                task_type=task_type,
                dependencies=[
                    task_id_map.get(dep, dep)
                    for dep in list(dict.fromkeys(draft_task.dependencies))
                ],
                input_payload=draft_task.input_payload
                or default_payload_for_task_type(task_type, stage_name),
                timeout_seconds=draft_task.timeout_seconds or request.timeout_seconds,
                retry_policy=request.retry_policy,
                idempotency_key=draft_task.idempotency_key
                or default_idempotency_key(task_type, task_id_map[draft_task.task_id]),
                metadata={
                    **stringify_map(draft_task.metadata),
                    "planner_origin": "gemini",
                    "planner_backend": "gemini",
                    "gemini_model": GEMINI_MODEL,
                    "intent_stage": stage_name,
                },
            )
        )

    workflow = WorkflowSpec(
        name=request.name,
        metadata={
            "planner": "helios-planner",
            "intent": request.intent,
            "planner_backend": "gemini",
            "gemini_model": GEMINI_MODEL,
        },
        tasks=tasks,
    )
    planning_notes = [
        "Planner used Gemini structured JSON output to propose the static DAG.",
        (
            "Gemini suggestions are validated and constrained to trusted task "
            "handler types before returning."
        ),
    ]
    planning_notes.extend(draft.planning_notes)
    return IntentResponse(
        workflow=workflow,
        planning_notes=planning_notes,
        scheduler_hints={
            "recommended_parallelism": max(
                1, min(8, int(draft.scheduler_hints.get("recommended_parallelism", 2)))
            ),
            "batch_size": max(1, int(draft.scheduler_hints.get("batch_size", 8))),
            "backpressure_mode": str(
                draft.scheduler_hints.get("backpressure_mode", "protect_workers")
            ),
            "planner_backend": "gemini",
        },
        created_at=now_local(),
    )


def optimize_with_gemini(request: OptimizationRequest) -> OptimizationResponse | None:
    prompt = build_optimization_prompt(request)
    try:
        payload = call_gemini_json("optimize", prompt, optimization_response_schema())
        draft = GeminiOptimizationDraft.model_validate(payload)
    except Exception as exc:
        LOGGER.warning("gemini optimization failed: %s", exc)
        return None

    recommended = max(1, min(16, draft.recommended_concurrency))
    return OptimizationResponse(
        recommended_concurrency=recommended,
        batching_recommendation=draft.batching_recommendation,
        stragglers=draft.stragglers,
        notes=[
            "Planner used Gemini structured output for advisory optimization guidance.",
            (
                "Operators should review Gemini recommendations before changing "
                "concurrency in production."
            ),
            *draft.notes,
        ],
    )


def analyze_failure_with_gemini(
    request: FailureAnalysisRequest,
    runbook_matches: list[RunbookMatch],
) -> FailureAnalysisResponse | None:
    prompt = build_failure_prompt(request, runbook_matches)
    try:
        payload = call_gemini_json("failure_analysis", prompt, failure_response_schema())
        draft = GeminiFailureDraft.model_validate(payload)
    except Exception as exc:
        LOGGER.warning("gemini failure analysis failed: %s", exc)
        return None

    return FailureAnalysisResponse(
        summary=draft.summary,
        failure_class=safe_slug(draft.failure_class) or "unknown_failure",
        likely_root_causes=draft.likely_root_causes,
        recovery_actions=draft.recovery_actions,
        affected_tasks=draft.affected_tasks,
        runbook_matches=runbook_matches,
        confidence=max(0.0, min(1.0, draft.confidence)),
        backend="gemini",
        generated_at=now_local(),
        raw_snapshot=request.workflow if request.include_raw_snapshot else None,
    )


def call_gemini_json(
    operation: str, prompt: str, response_schema: dict[str, Any]
) -> dict[str, Any]:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY is not configured")
    endpoint = (
        f"{GEMINI_API_BASE}/models/{GEMINI_MODEL}:generateContent?key={parse.quote(GEMINI_API_KEY)}"
    )
    body = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
            "responseSchema": response_schema,
        },
    }
    req = urlrequest.Request(
        endpoint,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.perf_counter()
    GENAI_TOKEN_ESTIMATE.labels(operation, GEMINI_MODEL, "input").inc(estimate_tokens(prompt))
    try:
        with urlrequest.urlopen(req, timeout=GEMINI_TIMEOUT_SECONDS) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        GENAI_REQUESTS.labels(operation, GEMINI_MODEL, "error").inc()
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"gemini http error {exc.code}: {detail}") from exc
    except error.URLError as exc:
        GENAI_REQUESTS.labels(operation, GEMINI_MODEL, "error").inc()
        raise RuntimeError(f"gemini network error: {exc.reason}") from exc
    finally:
        GENAI_LATENCY_SECONDS.labels(operation, GEMINI_MODEL).observe(time.perf_counter() - started)

    parts = raw.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    text = next((part.get("text") for part in parts if part.get("text")), "")
    if not text:
        GENAI_REQUESTS.labels(operation, GEMINI_MODEL, "error").inc()
        raise RuntimeError("gemini returned no structured text payload")
    GENAI_TOKEN_ESTIMATE.labels(operation, GEMINI_MODEL, "output").inc(estimate_tokens(text))
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        GENAI_REQUESTS.labels(operation, GEMINI_MODEL, "error").inc()
        raise RuntimeError("gemini returned non-object JSON")
    GENAI_REQUESTS.labels(operation, GEMINI_MODEL, "ok").inc()
    return parsed


def build_intent_prompt(request: IntentRequest) -> str:
    return (
        "You are planning a static DAG for the Helios distributed task orchestration engine.\n"
        "Return only JSON that matches the response schema.\n"
        f"Workflow name: {request.name}\n"
        f"Intent: {request.intent}\n"
        f"User-provided stages: {', '.join(request.stages) if request.stages else 'none'}\n"
        f"Default task timeout seconds: {request.timeout_seconds}\n"
        f"Retry policy: max_attempts={request.retry_policy.max_attempts}, "
        f"initial_backoff_seconds={request.retry_policy.initial_backoff_seconds}, "
        f"max_backoff_seconds={request.retry_policy.max_backoff_seconds}, "
        f"multiplier={request.retry_policy.multiplier}\n"
        f"Supported trusted task types: {', '.join(SUPPORTED_TASK_TYPES)}\n"
        "Use only these task type behaviors:\n"
        "- validate_payload: validates records using required fields, field types, and uniqueness\n"
        "- transform_records: applies deterministic field selection, rename, normalization, "
        "rounding, and enrichment\n"
        "- model_inference: runs trusted rule-based inference and can simulate retryable "
        "model errors\n"
        "- aggregate_metrics: aggregates inference predictions into counts and score metrics\n"
        "- write_artifact: writes an idempotent local or manifest artifact\n"
        "- notify_webhook: sends or dry-runs an outbound webhook notification\n"
        "Prefer 3 to 6 tasks. Dependencies must refer only to earlier task_ids. "
        "Keep the workflow a valid DAG with no cycles and no unsupported task types."
    )


def build_optimization_prompt(request: OptimizationRequest) -> str:
    signal_lines = []
    for signal in request.signals:
        signal_lines.append(
            f"- task_type={signal.task_type}, p95_ms={signal.p95_ms}, "
            f"retry_rate={signal.retry_rate}"
        )
    joined_signals = "\n".join(signal_lines) if signal_lines else "- no signals provided"
    return (
        "You are analyzing Helios execution telemetry to suggest advisory scheduler tuning.\n"
        "Return only JSON that matches the response schema.\n"
        f"Workflow name: {request.workflow_name}\n"
        f"Current concurrency: {request.current_concurrency}\n"
        "Signals:\n"
        f"{joined_signals}\n"
        "Recommend a cautious concurrency adjustment, list stragglers, and "
        "describe batch guidance. "
        "Do not recommend concurrency below 1 or above 16."
    )


def intent_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["tasks", "planning_notes", "scheduler_hints"],
        "properties": {
            "tasks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["task_id", "task_type", "dependencies", "input_payload"],
                    "properties": {
                        "task_id": {"type": "string"},
                        "task_type": {"type": "string"},
                        "dependencies": {"type": "array", "items": {"type": "string"}},
                        "input_payload": {"type": "object"},
                        "timeout_seconds": {"type": "integer"},
                        "metadata": {"type": "object"},
                        "idempotency_key": {"type": "string"},
                    },
                },
            },
            "planning_notes": {"type": "array", "items": {"type": "string"}},
            "scheduler_hints": {
                "type": "object",
                "required": ["recommended_parallelism", "batch_size", "backpressure_mode"],
                "properties": {
                    "recommended_parallelism": {"type": "integer"},
                    "batch_size": {"type": "integer"},
                    "backpressure_mode": {"type": "string"},
                },
            },
        },
    }


def optimization_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["recommended_concurrency", "batching_recommendation", "stragglers", "notes"],
        "properties": {
            "recommended_concurrency": {"type": "integer"},
            "batching_recommendation": {"type": "string"},
            "stragglers": {"type": "array", "items": {"type": "string"}},
            "notes": {"type": "array", "items": {"type": "string"}},
        },
    }


def failure_response_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "summary",
            "failure_class",
            "likely_root_causes",
            "recovery_actions",
            "affected_tasks",
            "confidence",
        ],
        "properties": {
            "summary": {"type": "string"},
            "failure_class": {"type": "string"},
            "likely_root_causes": {"type": "array", "items": {"type": "string"}},
            "recovery_actions": {"type": "array", "items": {"type": "string"}},
            "affected_tasks": {"type": "array", "items": {"type": "string"}},
            "confidence": {"type": "number"},
        },
    }


def now_local() -> datetime:
    return datetime.now(LOCAL_TIMEZONE)


def build_failure_prompt(
    request: FailureAnalysisRequest, runbook_matches: list[RunbookMatch]
) -> str:
    snapshot = compact_workflow_snapshot(request.workflow)
    runbooks = (
        "\n\n".join(
            f"Runbook: {match.title}\nPath: {match.path}\nExcerpt: {match.excerpt}"
            for match in runbook_matches
        )
        or "No runbooks matched."
    )
    return (
        "You are the Helios AI failure analyzer for a distributed task execution engine.\n"
        "Return only JSON matching the response schema. Do not invent state transitions.\n"
        "Classify the most likely failure mode, identify affected tasks, and "
        "propose operator-reviewed recovery actions.\n"
        "Important contracts: Helios uses at-least-once execution, PostgreSQL is "
        "source of truth, and stale results must not corrupt active attempts.\n"
        f"Workflow snapshot:\n{json.dumps(snapshot, indent=2, default=str)}\n\n"
        f"Retrieved runbooks:\n{runbooks}\n"
    )


def heuristic_failure_analysis(
    request: FailureAnalysisRequest,
    runbook_matches: list[RunbookMatch],
) -> FailureAnalysisResponse:
    tasks = (
        request.workflow.get("tasks", []) if isinstance(request.workflow.get("tasks"), list) else []
    )
    events = (
        request.workflow.get("events", [])
        if isinstance(request.workflow.get("events"), list)
        else []
    )
    workflow_state = str(request.workflow.get("state", "")).lower()
    affected = [
        str(task.get("task_id"))
        for task in tasks
        if str(task.get("state", "")).lower()
        in {"failed", "timed_out", "retry_wait", "leased", "running"}
    ]
    event_text = " ".join(
        " ".join(str(event.get(key, "")) for key in ("reason", "description", "new_state", "actor"))
        for event in events[:80]
    ).lower()
    task_errors = " ".join(str(task.get("last_error", "")) for task in tasks).lower()
    corpus = f"{event_text} {task_errors}"

    failure_class = "workflow_degraded"
    likely_causes = [
        "Workflow contains non-terminal or failed task states that require operator inspection."
    ]
    actions = [
        "Inspect failed task attempts and verify whether the error is retryable.",
        "Confirm workers are healthy before retrying or resubmitting the workflow.",
    ]
    confidence = 0.58

    if workflow_state == "succeeded" and not affected:
        failure_class = "no_failure_detected"
        likely_causes = ["The workflow is already in a terminal succeeded state."]
        actions = [
            "No recovery action is required.",
            (
                "Use the audit timeline and metrics to explain the successful "
                "execution path if this was an interview demo."
            ),
        ]
        confidence = 0.93
    elif (
        "expired" in corpus
        or "abandoned" in corpus
        or "dead worker" in corpus
        or "lease expired" in corpus
    ):
        failure_class = "lease_expired_or_worker_lost"
        likely_causes = [
            "A worker likely stopped heartbeating or exceeded its task lease.",
            "The active attempt may have been recovered under at-least-once semantics.",
        ]
        actions = [
            "Check worker heartbeat freshness and health state.",
            "Verify the task was requeued with a new attempt instead of accepting a stale result.",
            "Review lease duration and task timeout for the affected task type.",
        ]
        confidence = 0.78
    elif "timeout" in corpus or any(str(task.get("state")) == "timed_out" for task in tasks):
        failure_class = "task_timeout"
        likely_causes = [
            "One or more tasks exceeded timeout or lease recovery limits.",
            (
                "Payload size, downstream latency, or worker saturation may be "
                "too high for the configured timeout."
            ),
        ]
        actions = [
            "Compare task duration against timeout_seconds and p95 latency.",
            "Reduce batch size or increase timeout for the affected task type.",
            "Confirm retry budget is sufficient for transient downstream latency.",
        ]
        confidence = 0.74
    elif "retry" in corpus or any(str(task.get("state")) == "retry_wait" for task in tasks):
        failure_class = "elevated_retry_rate"
        likely_causes = [
            "A retryable handler or downstream dependency is intermittently failing.",
            "The workflow is protected by bounded exponential backoff.",
        ]
        actions = [
            "Inspect retryable error messages and attempt count.",
            (
                "Throttle concurrency for the affected task type if retries "
                "cluster around the same dependency."
            ),
            "Let retry_wait tasks promote naturally unless the incident requires cancellation.",
        ]
        confidence = 0.72
    elif "nats" in corpus or "dispatch" in corpus:
        failure_class = "dispatch_transport_issue"
        likely_causes = [
            "Task dispatch through NATS may be delayed or unavailable.",
            "Durable workflow state should remain recoverable from PostgreSQL.",
        ]
        actions = [
            "Check NATS health and scheduler assignment metrics.",
            (
                "Confirm ready tasks remain persisted and can be rediscovered "
                "after transport recovery."
            ),
        ]
        confidence = 0.7

    if not affected:
        affected = [
            str(task.get("task_id"))
            for task in tasks
            if str(task.get("state", "")).lower() not in {"succeeded", "cancelled"}
        ][:5]

    return FailureAnalysisResponse(
        summary=summarize_failure(request.workflow, failure_class, affected),
        failure_class=failure_class,
        likely_root_causes=likely_causes,
        recovery_actions=actions,
        affected_tasks=affected[:10],
        runbook_matches=runbook_matches,
        confidence=confidence,
        backend="heuristic",
        generated_at=now_local(),
        raw_snapshot=request.workflow if request.include_raw_snapshot else None,
    )


def build_failure_query(request: FailureAnalysisRequest) -> str:
    if request.runbook_query:
        return request.runbook_query
    snapshot = compact_workflow_snapshot(request.workflow)
    return json.dumps(snapshot, default=str)


def retrieve_runbooks(query: str, limit: int = 3) -> list[RunbookMatch]:
    if not RUNBOOK_DIR.exists():
        RAG_RETRIEVALS.labels("missing_dir").inc()
        return []
    query_terms = tokenize(query)
    matches: list[RunbookMatch] = []
    for path in sorted(RUNBOOK_DIR.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        title = extract_title(text, path)
        terms = tokenize(text)
        overlap = len(query_terms.intersection(terms))
        if overlap == 0:
            continue
        score = overlap / max(1, len(query_terms))
        matches.append(
            RunbookMatch(
                title=title,
                path=str(path),
                score=round(score, 4),
                excerpt=best_excerpt(text, query_terms),
            )
        )
    matches.sort(key=lambda match: match.score, reverse=True)
    RAG_RETRIEVALS.labels("ok" if matches else "empty").inc()
    return matches[:limit]


def compact_workflow_snapshot(workflow: dict[str, Any]) -> dict[str, Any]:
    tasks = workflow.get("tasks", []) if isinstance(workflow.get("tasks"), list) else []
    events = workflow.get("events", []) if isinstance(workflow.get("events"), list) else []
    return {
        "workflow_id": workflow.get("workflow_id", workflow.get("WorkflowID", "")),
        "name": workflow.get("name", ""),
        "state": workflow.get("state", ""),
        "tasks": [
            {
                "task_id": task.get("task_id"),
                "task_type": task.get("task_type"),
                "state": task.get("state"),
                "attempt": task.get("attempt"),
                "max_attempts": task.get("max_attempts"),
                "assigned_worker": task.get("assigned_worker"),
                "last_error": task.get("last_error"),
            }
            for task in tasks[:50]
        ],
        "recent_events": [
            {
                "task_id": event.get("task_id"),
                "actor": event.get("actor"),
                "old_state": event.get("old_state"),
                "new_state": event.get("new_state"),
                "reason": event.get("reason"),
                "description": event.get("description"),
            }
            for event in events[:40]
        ],
    }


def summarize_failure(workflow: dict[str, Any], failure_class: str, affected: list[str]) -> str:
    workflow_id = workflow.get("workflow_id", "unknown workflow")
    state = workflow.get("state", "unknown")
    task_text = ", ".join(affected[:3]) if affected else "no single task"
    return (
        f"{workflow_id} is {state}; classified as {failure_class} with {task_text} needing review."
    )


def tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_-]{2,}", text.lower())
        if token not in {"the", "and", "for", "with", "this", "that", "from", "into"}
    }


def extract_title(text: str, path: Path) -> str:
    for line in text.splitlines():
        if line.startswith("#"):
            return line.lstrip("# ").strip()
    return path.stem.replace("-", " ").title()


def best_excerpt(text: str, query_terms: set[str], max_chars: int = 420) -> str:
    paragraphs = [paragraph.strip() for paragraph in text.split("\n\n") if paragraph.strip()]
    if not paragraphs:
        return ""
    best = max(paragraphs, key=lambda paragraph: len(tokenize(paragraph).intersection(query_terms)))
    best = re.sub(r"\s+", " ", best)
    return best[:max_chars].rstrip()


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


def safe_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_")


def fallback_notes(reason: str) -> list[str]:
    if PLANNER_BACKEND == "heuristic":
        return []
    if not GEMINI_API_KEY:
        return [
            f"Gemini backend requested for {reason}, but GEMINI_API_KEY is not "
            "configured; used heuristic fallback."
        ]
    return [
        f"Gemini backend was unavailable during {reason}; used heuristic fallback "
        "to keep the planner responsive."
    ]


def derive_stages(intent: str) -> list[str]:
    default_pipeline = ["ingest", "validate", "transform", "persist"]
    tokens = [token.strip(" ,.") for token in intent.lower().split()]
    discovered = []
    for keyword in [
        "fetch",
        "validate",
        "normalize",
        "transform",
        "infer",
        "inference",
        "score",
        "aggregate",
        "write",
        "persist",
        "notify",
    ]:
        if keyword in tokens:
            discovered.append(keyword)
    return discovered or default_pipeline


def map_stage_to_task_type(stage: str) -> str:
    lowered = stage.lower()
    if lowered in {"validate", "validation"}:
        return "validate_payload"
    if lowered in {"features", "feature", "enrich", "transform", "normalize"}:
        return "transform_records"
    if lowered in {"score", "scoring", "risk", "infer", "inference", "model"}:
        return "model_inference"
    if lowered in {"aggregate", "aggregation"}:
        return "aggregate_metrics"
    if lowered in {"persist", "write", "store"}:
        return "write_artifact"
    if lowered in {"notify", "notification", "webhook"}:
        return "notify_webhook"
    if lowered in {"convert"}:
        return "transform_records"
    return "validate_payload"


def default_payload_for_task_type(task_type: str, stage: str) -> dict[str, Any]:
    if task_type == "validate_payload":
        return {
            "records": sample_order_records(),
            "required_fields": ["id", "amount", "currency", "country"],
            "field_types": {
                "id": "string",
                "amount": "number",
                "currency": "string",
                "country": "string",
            },
            "unique_key": "id",
        }
    if task_type == "transform_records":
        return {
            "records": sample_order_records(),
            "rename_fields": {"amount": "order_amount"},
            "uppercase_fields": ["currency", "country"],
            "lowercase_fields": ["channel"],
            "round_fields": {"order_amount": 2},
            "add_fields": {"pipeline_version": "planner-orders-v1"},
        }
    if task_type == "model_inference":
        return {
            "model_name": "planner-risk-rules-v1",
            "records": sample_normalized_orders(),
            "rules": [
                {
                    "field": "order_amount",
                    "operator": "gte",
                    "value": 1000,
                    "score": 0.45,
                    "contributor": "large_order",
                },
                {
                    "field": "country",
                    "operator": "in",
                    "values": ["NG", "IR", "KP"],
                    "score": 0.3,
                    "contributor": "high_risk_country",
                },
            ],
        }
    if task_type == "aggregate_metrics":
        return {"predictions": sample_predictions()}
    if task_type == "write_artifact":
        return {
            "sink": "manifest",
            "dataset": "planner_generated",
            "artifact": {"stage": stage, "status": "ready", "source": "planner"},
        }
    if task_type == "notify_webhook":
        return {
            "dry_run": True,
            "method": "POST",
            "url": "https://ops.example.local/hooks/helios",
            "body": {"stage": stage, "status": "ready"},
        }
    return {"records": sample_order_records()}


def default_idempotency_key(task_type: str, task_id: str) -> str | None:
    if task_type in {"write_artifact", "notify_webhook"}:
        return f"planner-{task_id}-artifact"
    return None


def sample_order_records() -> list[dict[str, Any]]:
    return [
        {
            "id": "ord-1",
            "amount": 42.75,
            "currency": "usd",
            "country": "us",
            "channel": "pos",
        },
        {
            "id": "ord-2",
            "amount": 1275.2,
            "currency": "usd",
            "country": "ng",
            "channel": "web",
        },
    ]


def sample_normalized_orders() -> list[dict[str, Any]]:
    return [
        {
            "id": "ord-1",
            "order_amount": 42.75,
            "currency": "USD",
            "country": "US",
            "channel": "pos",
        },
        {
            "id": "ord-2",
            "order_amount": 1275.2,
            "currency": "USD",
            "country": "NG",
            "channel": "web",
        },
    ]


def sample_predictions() -> list[dict[str, Any]]:
    return [
        {
            "id": "ord-1",
            "score": 0.05,
            "decision": "approve",
            "contributors": ["base"],
        },
        {
            "id": "ord-2",
            "score": 0.9,
            "decision": "block",
            "contributors": ["base", "large_order", "high_risk_country"],
        },
    ]


def stage_slug(stage: str, index: int) -> str:
    normalized = "".join(
        ch if ch.isalnum() or ch == "-" else "-" for ch in stage.lower().replace(" ", "-")
    )
    normalized = normalized.strip("-") or f"task-{index + 1}"
    return f"{index + 1:02d}-{normalized}"


def stringify_map(values: dict[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in values.items()}
