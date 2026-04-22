from __future__ import annotations

import json
import os
import socket
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import redis
from croniter import croniter
from sqlalchemy import Boolean, Engine, Integer, MetaData, String, Text, create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_iso8601(value: datetime | None) -> str:
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def parse_iso8601(value: str | None) -> datetime | None:
    token = str(value or "").strip()
    if not token:
        return None
    parsed = datetime.fromisoformat(token)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class Base(DeclarativeBase):
    metadata = MetaData()


class JobRecord(Base):
    __tablename__ = "jobs"

    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    flow_type: Mapped[str] = mapped_column(String(32), nullable=False)
    demandante: Mapped[str] = mapped_column(String(64), nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    artifacts_root: Mapped[str] = mapped_column(Text, nullable=False)
    runner_id: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    queue_name: Mapped[str] = mapped_column(String(64), nullable=False, default="sap-default")
    idempotency_key: Mapped[str | None] = mapped_column(String(255), nullable=True, default=None, unique=True)
    result_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    manifest_path: Mapped[str] = mapped_column(Text, nullable=False, default="")
    error_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    claimed_at: Mapped[datetime | None] = mapped_column(nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)


class WorkflowRunRecord(Base):
    __tablename__ = "workflow_runs"

    workflow_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    workflow_type: Mapped[str] = mapped_column(String(64), nullable=False)
    demandante: Mapped[str] = mapped_column(String(64), nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="running")
    reference: Mapped[str] = mapped_column(String(32), nullable=False, default="")
    payload_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    current_step: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)


class WorkflowStepRecord(Base):
    __tablename__ = "workflow_steps"

    step_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    workflow_id: Mapped[str] = mapped_column(String(64), nullable=False)
    step_name: Mapped[str] = mapped_column(String(64), nullable=False)
    job_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    order_index: Mapped[int] = mapped_column(Integer, nullable=False)
    input_artifacts_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    output_artifacts_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    started_at: Mapped[datetime | None] = mapped_column(nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)


class ArtifactRecord(Base):
    __tablename__ = "artifacts"

    artifact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_name: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[str] = mapped_column(String(64), nullable=False, default="file")
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    path: Mapped[str] = mapped_column(Text, nullable=False)
    producer_job_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)


class ScheduleRecord(Base):
    __tablename__ = "schedules"

    schedule_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    flow_type: Mapped[str] = mapped_column(String(32), nullable=False)
    demandante: Mapped[str] = mapped_column(String(64), nullable=False)
    cron_expression: Mapped[str] = mapped_column(String(128), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, default="America/Bahia")
    payload_template_json: Mapped[str] = mapped_column(Text, nullable=False)
    misfire_policy: Mapped[str] = mapped_column(String(32), nullable=False, default="catch_up")
    queue_name: Mapped[str] = mapped_column(String(64), nullable=False, default="sap-default")
    last_run_at: Mapped[datetime | None] = mapped_column(nullable=True)
    next_run_at: Mapped[datetime] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)


class RunnerRecord(Base):
    __tablename__ = "runners"

    runner_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    capabilities_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    current_job_id: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    sap_available: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    desktop_session_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_heartbeat_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    status_message: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(nullable=False, default=utc_now)


@dataclass(frozen=True)
class ControlPlaneSettings:
    database_url: str
    redis_url: str
    queue_name: str
    scheduler_poll_seconds: float
    runner_poll_seconds: float
    runner_heartbeat_seconds: float
    runner_id: str
    output_root: Path

    @classmethod
    def from_env(cls) -> "ControlPlaneSettings":
        output_root = Path(os.environ.get("SAP_OUTPUT_ROOT", "output")).expanduser().resolve()
        database_url = str(
            os.environ.get(
                "DATABASE_URL",
                f"sqlite+pysqlite:///{(output_root / 'control_plane.db').as_posix()}",
            )
        ).strip()
        return cls(
            database_url=database_url,
            redis_url=str(os.environ.get("REDIS_URL", "")).strip(),
            queue_name=str(os.environ.get("SAP_QUEUE_NAME", "sap-default")).strip() or "sap-default",
            scheduler_poll_seconds=max(1.0, float(os.environ.get("SAP_SCHEDULER_POLL_SECONDS", "15"))),
            runner_poll_seconds=max(1.0, float(os.environ.get("SAP_RUNNER_POLL_SECONDS", "5"))),
            runner_heartbeat_seconds=max(5.0, float(os.environ.get("SAP_RUNNER_HEARTBEAT_SECONDS", "15"))),
            runner_id=str(os.environ.get("SAP_RUNNER_ID", socket.gethostname())).strip() or socket.gethostname(),
            output_root=output_root,
        )


class JobQueue(Protocol):
    def enqueue(self, *, queue_name: str, job_id: str) -> None: ...

    def dequeue(self, *, queue_name: str, timeout_seconds: float) -> str | None: ...


class InMemoryJobQueue:
    def __init__(self) -> None:
        self._queues: dict[str, list[str]] = {}

    def enqueue(self, *, queue_name: str, job_id: str) -> None:
        self._queues.setdefault(queue_name, []).append(job_id)

    def dequeue(self, *, queue_name: str, timeout_seconds: float) -> str | None:
        queue = self._queues.setdefault(queue_name, [])
        if queue:
            return queue.pop(0)
        if timeout_seconds > 0:
            time.sleep(timeout_seconds)
        return None


class RedisJobQueue:
    def __init__(self, redis_url: str) -> None:
        self._client = redis.Redis.from_url(redis_url, decode_responses=True)

    def enqueue(self, *, queue_name: str, job_id: str) -> None:
        self._client.rpush(queue_name, job_id)

    def dequeue(self, *, queue_name: str, timeout_seconds: float) -> str | None:
        result = self._client.blpop(queue_name, timeout=max(1, int(timeout_seconds)))
        if result is None:
            return None
        _, job_id = result
        return str(job_id)


def build_engine(database_url: str) -> Engine:
    if database_url.startswith("sqlite"):
        return create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            future=True,
        )
    return create_engine(database_url, future=True)


def build_queue(redis_url: str) -> JobQueue:
    if redis_url:
        return RedisJobQueue(redis_url)
    return InMemoryJobQueue()


@dataclass(frozen=True)
class JobEnvelope:
    job_id: str
    flow_type: str
    demandante: str
    run_id: str
    status: str
    scheduled_at: str
    artifacts_root: str
    runner_id: str
    queue_name: str
    payload: dict[str, Any]
    idempotency_key: str
    result: dict[str, Any]
    manifest_path: str
    error_message: str
    created_at: str
    updated_at: str
    claimed_at: str
    started_at: str
    finished_at: str


@dataclass(frozen=True)
class ScheduleEnvelope:
    schedule_id: str
    enabled: bool
    flow_type: str
    demandante: str
    cron_expression: str
    timezone: str
    payload_template: dict[str, Any]
    misfire_policy: str
    queue_name: str
    last_run_at: str
    next_run_at: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class RunnerEnvelope:
    runner_id: str
    hostname: str
    capabilities: list[str]
    current_job_id: str
    sap_available: bool
    desktop_session_ready: bool
    last_heartbeat_at: str
    status_message: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class ArtifactEnvelope:
    artifact_id: str
    run_id: str
    artifact_name: str
    kind: str
    sha256: str
    size_bytes: int
    path: str
    producer_job_id: str
    created_at: str


@dataclass(frozen=True)
class WorkflowStepEnvelope:
    step_id: str
    workflow_id: str
    step_name: str
    job_id: str
    status: str
    order_index: int
    input_artifacts: list[str]
    output_artifacts: list[str]
    created_at: str
    updated_at: str
    started_at: str
    finished_at: str


@dataclass(frozen=True)
class WorkflowRunEnvelope:
    workflow_id: str
    workflow_type: str
    demandante: str
    run_id: str
    status: str
    reference: str
    payload: dict[str, Any]
    current_step: str
    steps: list[dict[str, Any]]
    artifacts: list[dict[str, Any]]
    created_at: str
    updated_at: str
    started_at: str
    finished_at: str


class ControlPlaneService:
    def __init__(
        self,
        *,
        settings: ControlPlaneSettings,
        engine: Engine | None = None,
        queue: JobQueue | None = None,
    ) -> None:
        self.settings = settings
        self.engine = engine or build_engine(settings.database_url)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.queue = queue or build_queue(settings.redis_url)
        Base.metadata.create_all(self.engine)

    def _to_job_envelope(self, record: JobRecord) -> JobEnvelope:
        return JobEnvelope(
            job_id=record.job_id,
            flow_type=record.flow_type,
            demandante=record.demandante,
            run_id=record.run_id,
            status=record.status,
            scheduled_at=to_iso8601(record.scheduled_at),
            artifacts_root=record.artifacts_root,
            runner_id=record.runner_id,
            queue_name=record.queue_name,
            payload=json.loads(record.payload_json),
            idempotency_key=str(record.idempotency_key or ""),
            result=json.loads(record.result_json or "{}"),
            manifest_path=record.manifest_path,
            error_message=record.error_message,
            created_at=to_iso8601(record.created_at),
            updated_at=to_iso8601(record.updated_at),
            claimed_at=to_iso8601(record.claimed_at),
            started_at=to_iso8601(record.started_at),
            finished_at=to_iso8601(record.finished_at),
        )

    def _to_schedule_envelope(self, record: ScheduleRecord) -> ScheduleEnvelope:
        return ScheduleEnvelope(
            schedule_id=record.schedule_id,
            enabled=record.enabled,
            flow_type=record.flow_type,
            demandante=record.demandante,
            cron_expression=record.cron_expression,
            timezone=record.timezone,
            payload_template=json.loads(record.payload_template_json),
            misfire_policy=record.misfire_policy,
            queue_name=record.queue_name,
            last_run_at=to_iso8601(record.last_run_at),
            next_run_at=to_iso8601(record.next_run_at),
            created_at=to_iso8601(record.created_at),
            updated_at=to_iso8601(record.updated_at),
        )

    def _to_runner_envelope(self, record: RunnerRecord) -> RunnerEnvelope:
        return RunnerEnvelope(
            runner_id=record.runner_id,
            hostname=record.hostname,
            capabilities=json.loads(record.capabilities_json or "[]"),
            current_job_id=record.current_job_id,
            sap_available=record.sap_available,
            desktop_session_ready=record.desktop_session_ready,
            last_heartbeat_at=to_iso8601(record.last_heartbeat_at),
            status_message=record.status_message,
            created_at=to_iso8601(record.created_at),
            updated_at=to_iso8601(record.updated_at),
        )

    def _to_artifact_envelope(self, record: ArtifactRecord) -> ArtifactEnvelope:
        return ArtifactEnvelope(
            artifact_id=record.artifact_id,
            run_id=record.run_id,
            artifact_name=record.artifact_name,
            kind=record.kind,
            sha256=record.sha256,
            size_bytes=record.size_bytes,
            path=record.path,
            producer_job_id=record.producer_job_id,
            created_at=to_iso8601(record.created_at),
        )

    def _to_step_envelope(self, record: WorkflowStepRecord) -> WorkflowStepEnvelope:
        return WorkflowStepEnvelope(
            step_id=record.step_id,
            workflow_id=record.workflow_id,
            step_name=record.step_name,
            job_id=record.job_id,
            status=record.status,
            order_index=record.order_index,
            input_artifacts=json.loads(record.input_artifacts_json or "[]"),
            output_artifacts=json.loads(record.output_artifacts_json or "[]"),
            created_at=to_iso8601(record.created_at),
            updated_at=to_iso8601(record.updated_at),
            started_at=to_iso8601(record.started_at),
            finished_at=to_iso8601(record.finished_at),
        )

    def _to_workflow_envelope(
        self,
        record: WorkflowRunRecord,
        *,
        steps: list[WorkflowStepRecord] | None = None,
        artifacts: list[ArtifactRecord] | None = None,
    ) -> WorkflowRunEnvelope:
        return WorkflowRunEnvelope(
            workflow_id=record.workflow_id,
            workflow_type=record.workflow_type,
            demandante=record.demandante,
            run_id=record.run_id,
            status=record.status,
            reference=record.reference,
            payload=json.loads(record.payload_json or "{}"),
            current_step=record.current_step,
            steps=[self._to_step_envelope(step).__dict__ for step in steps or []],
            artifacts=[self._to_artifact_envelope(artifact).__dict__ for artifact in artifacts or []],
            created_at=to_iso8601(record.created_at),
            updated_at=to_iso8601(record.updated_at),
            started_at=to_iso8601(record.started_at),
            finished_at=to_iso8601(record.finished_at),
        )

    def _generate_run_id(self) -> str:
        return utc_now().strftime("%Y%m%dT%H%M%S")

    def _compute_next_run_at(self, *, cron_expression: str, timezone: str, now: datetime) -> datetime:
        zone = ZoneInfo(str(timezone).strip() or "America/Bahia")
        localized_now = now.astimezone(zone)
        next_local = croniter(cron_expression, localized_now).get_next(datetime)
        if next_local.tzinfo is None:
            next_local = next_local.replace(tzinfo=zone)
        return next_local.astimezone(UTC)

    def create_job(
        self,
        *,
        flow_type: str,
        demandante: str,
        payload: dict[str, Any],
        scheduled_at: datetime | None = None,
        queue_name: str | None = None,
        idempotency_key: str = "",
    ) -> JobEnvelope:
        scheduled = scheduled_at or utc_now()
        resolved_payload = dict(payload)
        run_id = str(resolved_payload.get("run_id", "")).strip() or self._generate_run_id()
        resolved_payload["run_id"] = run_id
        output_root = Path(str(resolved_payload.get("output_root", self.settings.output_root))).expanduser().resolve()
        resolved_payload["output_root"] = str(output_root)
        queue_token = str(queue_name or self.settings.queue_name).strip() or self.settings.queue_name
        job_id = uuid.uuid4().hex
        record = JobRecord(
            job_id=job_id,
            flow_type=str(flow_type).strip().lower(),
            demandante=str(demandante).strip().upper(),
            run_id=run_id,
            payload_json=json.dumps(resolved_payload, ensure_ascii=False, sort_keys=True),
            scheduled_at=scheduled,
            status="queued",
            artifacts_root=str(output_root / "runs" / run_id),
            runner_id="",
            queue_name=queue_token,
            idempotency_key=str(idempotency_key).strip() or None,
            result_json="{}",
            manifest_path="",
            error_message="",
            created_at=utc_now(),
            updated_at=utc_now(),
        )
        with self.SessionLocal() as session:
            try:
                session.add(record)
                session.commit()
            except IntegrityError:
                session.rollback()
                if record.idempotency_key:
                    existing = session.scalar(select(JobRecord).where(JobRecord.idempotency_key == record.idempotency_key))
                    if existing is not None:
                        return self._to_job_envelope(existing)
                raise
        self.queue.enqueue(queue_name=queue_token, job_id=job_id)
        return self._to_job_envelope(record)

    def create_workflow(
        self,
        *,
        workflow_type: str,
        demandante: str,
        payload: dict[str, Any],
        scheduled_at: datetime | None = None,
        idempotency_key: str = "",
    ) -> WorkflowRunEnvelope:
        resolved_type = str(workflow_type).strip().lower()
        if resolved_type != "sm_sala_mercado_daily":
            raise ValueError(f"Unsupported workflow_type={workflow_type}.")
        scheduled = scheduled_at or utc_now()
        resolved_payload = dict(payload)
        run_id = str(resolved_payload.get("run_id", "")).strip()
        if not run_id and idempotency_key:
            run_id = scheduled.strftime("%Y%m%dT%H%M%S")
        run_id = run_id or self._generate_run_id()
        resolved_payload["run_id"] = run_id
        resolved_payload.setdefault("output_root", str(self.settings.output_root))
        resolved_payload.setdefault("config_path", "sap_iw69_batch_config.json")
        reference = str(resolved_payload.get("reference", "")).strip()
        if reference == "current_month":
            reference = scheduled.astimezone(ZoneInfo("America/Bahia")).strftime("%Y%m")
        if reference and len(reference) == 6 and reference.isdigit():
            resolved_payload.setdefault("year", int(reference[:4]))
            resolved_payload.setdefault("month", int(reference[4:]))
        workflow_id = uuid.uuid4().hex
        now = utc_now()
        with self.SessionLocal() as session:
            existing = session.scalar(select(WorkflowRunRecord).where(WorkflowRunRecord.run_id == run_id))
            if existing is not None:
                return self.get_workflow(run_id=existing.run_id) or self._to_workflow_envelope(existing)
            workflow = WorkflowRunRecord(
                workflow_id=workflow_id,
                workflow_type=resolved_type,
                demandante=str(demandante).strip().upper(),
                run_id=run_id,
                status="running",
                reference=reference,
                payload_json=json.dumps(resolved_payload, ensure_ascii=False, sort_keys=True),
                current_step="sm_prepare_input",
                created_at=now,
                updated_at=now,
                started_at=now,
            )
            session.add(workflow)
            for index, step_name in enumerate(("sm_prepare_input", "sm_sap_extract", "sm_ingest_final"), start=1):
                session.add(
                    WorkflowStepRecord(
                        step_id=uuid.uuid4().hex,
                        workflow_id=workflow_id,
                        step_name=step_name,
                        status="pending",
                        order_index=index,
                        input_artifacts_json=json.dumps(_workflow_step_inputs(step_name)),
                        output_artifacts_json=json.dumps(_workflow_step_outputs(step_name)),
                        created_at=now,
                        updated_at=now,
                    )
                )
            session.commit()
        self._enqueue_workflow_step(workflow_id=workflow_id, step_name="sm_prepare_input")
        workflow = self.get_workflow(run_id=run_id)
        if workflow is None:
            raise RuntimeError(f"Workflow {run_id} was not created.")
        return workflow

    def get_workflow(self, *, run_id: str) -> WorkflowRunEnvelope | None:
        with self.SessionLocal() as session:
            workflow = session.scalar(select(WorkflowRunRecord).where(WorkflowRunRecord.run_id == run_id))
            if workflow is None:
                return None
            steps = session.scalars(
                select(WorkflowStepRecord)
                .where(WorkflowStepRecord.workflow_id == workflow.workflow_id)
                .order_by(WorkflowStepRecord.order_index.asc())
            ).all()
            artifacts = session.scalars(
                select(ArtifactRecord)
                .where(ArtifactRecord.run_id == workflow.run_id)
                .order_by(ArtifactRecord.created_at.asc())
            ).all()
            return self._to_workflow_envelope(workflow, steps=steps, artifacts=artifacts)

    def list_workflows(self, *, limit: int = 100) -> list[WorkflowRunEnvelope]:
        with self.SessionLocal() as session:
            workflows = session.scalars(
                select(WorkflowRunRecord).order_by(WorkflowRunRecord.created_at.desc()).limit(limit)
            ).all()
            envelopes: list[WorkflowRunEnvelope] = []
            for workflow in workflows:
                steps = session.scalars(
                    select(WorkflowStepRecord)
                    .where(WorkflowStepRecord.workflow_id == workflow.workflow_id)
                    .order_by(WorkflowStepRecord.order_index.asc())
                ).all()
                artifacts = session.scalars(select(ArtifactRecord).where(ArtifactRecord.run_id == workflow.run_id)).all()
                envelopes.append(self._to_workflow_envelope(workflow, steps=steps, artifacts=artifacts))
            return envelopes

    def register_artifact(
        self,
        *,
        run_id: str,
        artifact_name: str,
        path: Path,
        kind: str = "file",
        producer_job_id: str = "",
        sha256: str,
        size_bytes: int,
    ) -> ArtifactEnvelope:
        normalized_name = _normalize_artifact_name(artifact_name)
        now = utc_now()
        with self.SessionLocal() as session:
            existing = session.scalar(
                select(ArtifactRecord)
                .where(ArtifactRecord.run_id == run_id)
                .where(ArtifactRecord.artifact_name == normalized_name)
            )
            if existing is None:
                existing = ArtifactRecord(
                    artifact_id=uuid.uuid4().hex,
                    run_id=run_id,
                    artifact_name=normalized_name,
                    kind=str(kind).strip() or "file",
                    sha256=sha256,
                    size_bytes=size_bytes,
                    path=str(path),
                    producer_job_id=producer_job_id,
                    created_at=now,
                )
                session.add(existing)
            else:
                existing.kind = str(kind).strip() or "file"
                existing.sha256 = sha256
                existing.size_bytes = size_bytes
                existing.path = str(path)
                existing.producer_job_id = producer_job_id
                existing.created_at = now
            session.commit()
            return self._to_artifact_envelope(existing)

    def get_artifact(self, *, run_id: str, artifact_name: str) -> ArtifactEnvelope | None:
        normalized_name = _normalize_artifact_name(artifact_name)
        with self.SessionLocal() as session:
            row = session.scalar(
                select(ArtifactRecord)
                .where(ArtifactRecord.run_id == run_id)
                .where(ArtifactRecord.artifact_name == normalized_name)
            )
            return self._to_artifact_envelope(row) if row is not None else None

    def list_artifacts(self, *, run_id: str) -> list[ArtifactEnvelope]:
        with self.SessionLocal() as session:
            rows = session.scalars(
                select(ArtifactRecord)
                .where(ArtifactRecord.run_id == run_id)
                .order_by(ArtifactRecord.artifact_name.asc())
            ).all()
        return [self._to_artifact_envelope(row) for row in rows]

    def list_jobs(self, *, limit: int = 100) -> list[JobEnvelope]:
        with self.SessionLocal() as session:
            rows = session.scalars(select(JobRecord).order_by(JobRecord.created_at.desc()).limit(limit)).all()
        return [self._to_job_envelope(row) for row in rows]

    def get_job(self, *, job_id: str) -> JobEnvelope | None:
        with self.SessionLocal() as session:
            row = session.get(JobRecord, job_id)
            if row is None:
                return None
            return self._to_job_envelope(row)

    def cancel_job(self, *, job_id: str) -> JobEnvelope | None:
        with self.SessionLocal() as session:
            row = session.get(JobRecord, job_id)
            if row is None:
                return None
            if row.status not in {"queued", "claimed"}:
                return self._to_job_envelope(row)
            row.status = "cancelled"
            row.updated_at = utc_now()
            row.finished_at = utc_now()
            session.commit()
            return self._to_job_envelope(row)

    def claim_next_job(self, *, runner_id: str, queue_name: str | None = None, timeout_seconds: float = 5.0) -> JobEnvelope | None:
        queue_token = str(queue_name or self.settings.queue_name).strip() or self.settings.queue_name
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while time.monotonic() <= deadline:
            remaining = max(0.0, deadline - time.monotonic())
            job_id = self.queue.dequeue(queue_name=queue_token, timeout_seconds=remaining or 1.0)
            if not job_id:
                return None
            with self.SessionLocal() as session:
                row = session.get(JobRecord, job_id)
                if row is None or row.status != "queued":
                    continue
                row.status = "claimed"
                row.runner_id = runner_id
                row.claimed_at = utc_now()
                row.updated_at = utc_now()
                session.commit()
                return self._to_job_envelope(row)
        return None

    def mark_job_running(self, *, job_id: str, runner_id: str) -> JobEnvelope:
        with self.SessionLocal() as session:
            row = session.get(JobRecord, job_id)
            if row is None:
                raise RuntimeError(f"Job {job_id} not found.")
            row.status = "running"
            row.runner_id = runner_id
            row.started_at = utc_now()
            row.updated_at = utc_now()
            step = session.scalar(select(WorkflowStepRecord).where(WorkflowStepRecord.job_id == job_id))
            if step is not None:
                step.status = "running"
                step.started_at = row.started_at
                step.updated_at = row.updated_at
            session.commit()
            return self._to_job_envelope(row)

    def complete_job(
        self,
        *,
        job_id: str,
        status: str,
        result: dict[str, Any],
        manifest_path: str = "",
        error_message: str = "",
    ) -> JobEnvelope:
        if status not in {"success", "partial", "failed", "cancelled"}:
            raise ValueError(f"Unsupported job completion status: {status}")
        with self.SessionLocal() as session:
            row = session.get(JobRecord, job_id)
            if row is None:
                raise RuntimeError(f"Job {job_id} not found.")
            row.status = status
            row.result_json = json.dumps(result, ensure_ascii=False, sort_keys=True)
            row.manifest_path = manifest_path
            row.error_message = error_message
            row.finished_at = utc_now()
            row.updated_at = utc_now()
            session.commit()
            envelope = self._to_job_envelope(row)
        self._advance_workflow_after_job(job=envelope)
        return envelope

    def upsert_schedule(
        self,
        *,
        schedule_id: str,
        flow_type: str,
        demandante: str,
        cron_expression: str,
        timezone: str,
        payload_template: dict[str, Any],
        misfire_policy: str = "catch_up",
        enabled: bool = True,
        queue_name: str | None = None,
    ) -> ScheduleEnvelope:
        now = utc_now()
        queue_token = str(queue_name or self.settings.queue_name).strip() or self.settings.queue_name
        with self.SessionLocal() as session:
            row = session.get(ScheduleRecord, schedule_id)
            if row is None:
                row = ScheduleRecord(
                    schedule_id=schedule_id,
                    enabled=enabled,
                    flow_type=str(flow_type).strip().lower(),
                    demandante=str(demandante).strip().upper(),
                    cron_expression=str(cron_expression).strip(),
                    timezone=str(timezone).strip() or "America/Bahia",
                    payload_template_json=json.dumps(payload_template, ensure_ascii=False, sort_keys=True),
                    misfire_policy=str(misfire_policy).strip() or "catch_up",
                    queue_name=queue_token,
                    next_run_at=self._compute_next_run_at(
                        cron_expression=str(cron_expression).strip(),
                        timezone=str(timezone).strip() or "America/Bahia",
                        now=now,
                    ),
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.enabled = enabled
                row.flow_type = str(flow_type).strip().lower()
                row.demandante = str(demandante).strip().upper()
                row.cron_expression = str(cron_expression).strip()
                row.timezone = str(timezone).strip() or "America/Bahia"
                row.payload_template_json = json.dumps(payload_template, ensure_ascii=False, sort_keys=True)
                row.misfire_policy = str(misfire_policy).strip() or "catch_up"
                row.queue_name = queue_token
                row.next_run_at = self._compute_next_run_at(
                    cron_expression=row.cron_expression,
                    timezone=row.timezone,
                    now=now,
                )
                row.updated_at = now
            session.commit()
            return self._to_schedule_envelope(row)

    def list_schedules(self) -> list[ScheduleEnvelope]:
        with self.SessionLocal() as session:
            rows = session.scalars(select(ScheduleRecord).order_by(ScheduleRecord.schedule_id.asc())).all()
        return [self._to_schedule_envelope(row) for row in rows]

    def run_scheduler_tick(self, *, now: datetime | None = None) -> list[JobEnvelope]:
        current_time = now or utc_now()
        created_jobs: list[JobEnvelope] = []
        with self.SessionLocal() as session:
            due_rows = session.scalars(
                select(ScheduleRecord)
                .where(ScheduleRecord.enabled.is_(True))
                .where(ScheduleRecord.next_run_at <= current_time)
                .order_by(ScheduleRecord.next_run_at.asc())
            ).all()
            due_payloads = [
                {
                    "schedule_id": row.schedule_id,
                    "flow_type": row.flow_type,
                    "demandante": row.demandante,
                    "payload_template": json.loads(row.payload_template_json),
                    "scheduled_for": row.next_run_at,
                    "queue_name": row.queue_name,
                }
                for row in due_rows
            ]
            for row in due_rows:
                row.last_run_at = row.next_run_at
                row.next_run_at = self._compute_next_run_at(
                    cron_expression=row.cron_expression,
                    timezone=row.timezone,
                    now=row.next_run_at,
                )
                row.updated_at = current_time
            session.commit()

        for item in due_payloads:
            payload = dict(item["payload_template"])
            payload.setdefault("demandante", item["demandante"])
            payload.setdefault("output_root", str(self.settings.output_root))
            payload.setdefault("config_path", "sap_iw69_batch_config.json")
            idempotency_key = (
                f"{item['schedule_id']}:{item['flow_type']}:{item['demandante']}:{to_iso8601(item['scheduled_for'])}"
            )
            if str(item["flow_type"]).strip().lower().startswith("workflow:"):
                workflow_type = str(item["flow_type"]).split(":", 1)[1]
                workflow = self.create_workflow(
                    workflow_type=workflow_type,
                    demandante=str(item["demandante"]),
                    payload=payload,
                    scheduled_at=item["scheduled_for"],
                    idempotency_key=idempotency_key,
                )
                first_job_id = next((step["job_id"] for step in workflow.steps if step["job_id"]), "")
                if first_job_id:
                    first_job = self.get_job(job_id=first_job_id)
                    if first_job is not None:
                        created_jobs.append(first_job)
            else:
                job = self.create_job(
                    flow_type=str(item["flow_type"]),
                    demandante=str(item["demandante"]),
                    payload=payload,
                    scheduled_at=item["scheduled_for"],
                    queue_name=str(item["queue_name"]),
                    idempotency_key=idempotency_key,
                )
                created_jobs.append(job)
        return created_jobs

    def upsert_runner(
        self,
        *,
        runner_id: str,
        hostname: str,
        capabilities: list[str],
        current_job_id: str = "",
        sap_available: bool,
        desktop_session_ready: bool,
        status_message: str = "",
    ) -> RunnerEnvelope:
        now = utc_now()
        with self.SessionLocal() as session:
            row = session.get(RunnerRecord, runner_id)
            if row is None:
                row = RunnerRecord(
                    runner_id=runner_id,
                    hostname=hostname,
                    capabilities_json=json.dumps(capabilities, ensure_ascii=False, sort_keys=True),
                    current_job_id=current_job_id,
                    sap_available=sap_available,
                    desktop_session_ready=desktop_session_ready,
                    last_heartbeat_at=now,
                    status_message=status_message,
                    created_at=now,
                    updated_at=now,
                )
                session.add(row)
            else:
                row.hostname = hostname
                row.capabilities_json = json.dumps(capabilities, ensure_ascii=False, sort_keys=True)
                row.current_job_id = current_job_id
                row.sap_available = sap_available
                row.desktop_session_ready = desktop_session_ready
                row.last_heartbeat_at = now
                row.status_message = status_message
                row.updated_at = now
            session.commit()
            return self._to_runner_envelope(row)

    def list_runners(self) -> list[RunnerEnvelope]:
        with self.SessionLocal() as session:
            rows = session.scalars(select(RunnerRecord).order_by(RunnerRecord.runner_id.asc())).all()
        return [self._to_runner_envelope(row) for row in rows]

    def _enqueue_workflow_step(self, *, workflow_id: str, step_name: str) -> JobEnvelope:
        with self.SessionLocal() as session:
            workflow = session.get(WorkflowRunRecord, workflow_id)
            if workflow is None:
                raise RuntimeError(f"Workflow {workflow_id} not found.")
            step = session.scalar(
                select(WorkflowStepRecord)
                .where(WorkflowStepRecord.workflow_id == workflow_id)
                .where(WorkflowStepRecord.step_name == step_name)
            )
            if step is None:
                raise RuntimeError(f"Workflow step {step_name} not found.")
            if step.job_id:
                existing = session.get(JobRecord, step.job_id)
                if existing is not None:
                    return self._to_job_envelope(existing)
            payload = json.loads(workflow.payload_json or "{}")
            payload.update(
                {
                    "workflow_id": workflow.workflow_id,
                    "workflow_run_id": workflow.run_id,
                    "step_name": step_name,
                    "run_id": workflow.run_id,
                    "reference": workflow.reference,
                }
            )
        job = self.create_job(
            flow_type=step_name,
            demandante=workflow.demandante,
            payload=payload,
            queue_name=_workflow_step_queue(step_name),
            idempotency_key=f"workflow:{workflow.run_id}:{step_name}",
        )
        with self.SessionLocal() as session:
            workflow = session.get(WorkflowRunRecord, workflow_id)
            step = session.scalar(
                select(WorkflowStepRecord)
                .where(WorkflowStepRecord.workflow_id == workflow_id)
                .where(WorkflowStepRecord.step_name == step_name)
            )
            if workflow is None or step is None:
                raise RuntimeError(f"Workflow {workflow_id} changed while enqueueing {step_name}.")
            step.job_id = job.job_id
            step.status = "queued"
            step.updated_at = utc_now()
            workflow.current_step = step_name
            workflow.updated_at = utc_now()
            session.commit()
        return job

    def _advance_workflow_after_job(self, *, job: JobEnvelope) -> None:
        step_name = str(job.flow_type).strip().lower()
        if step_name not in {"sm_prepare_input", "sm_sap_extract", "sm_ingest_final"}:
            return
        workflow_id = str(job.payload.get("workflow_id", "")).strip()
        if not workflow_id:
            return
        next_step_name = ""
        with self.SessionLocal() as session:
            workflow = session.get(WorkflowRunRecord, workflow_id)
            if workflow is None or workflow.status in {"success", "failed", "cancelled"}:
                return
            step = session.scalar(
                select(WorkflowStepRecord)
                .where(WorkflowStepRecord.workflow_id == workflow_id)
                .where(WorkflowStepRecord.job_id == job.job_id)
            )
            if step is None:
                return
            now = utc_now()
            step.status = "success" if job.status in {"success", "partial"} else "failed"
            step.finished_at = now
            step.updated_at = now
            if step.status != "success":
                workflow.status = "failed"
                workflow.finished_at = now
                workflow.updated_at = now
                session.commit()
                return
            next_step = session.scalar(
                select(WorkflowStepRecord)
                .where(WorkflowStepRecord.workflow_id == workflow_id)
                .where(WorkflowStepRecord.order_index > step.order_index)
                .order_by(WorkflowStepRecord.order_index.asc())
            )
            if next_step is None:
                workflow.status = "success"
                workflow.current_step = ""
                workflow.finished_at = now
                workflow.updated_at = now
            else:
                next_step_name = next_step.step_name
                workflow.current_step = next_step.step_name
                workflow.updated_at = now
            session.commit()
        if next_step_name:
            self._enqueue_workflow_step(workflow_id=workflow_id, step_name=next_step_name)


def _workflow_step_queue(step_name: str) -> str:
    if step_name in {"sm_prepare_input", "sm_ingest_final"}:
        return "db-default"
    if step_name == "sm_sap_extract":
        return "sap-default"
    raise ValueError(f"Unsupported workflow step: {step_name}")


def _workflow_step_inputs(step_name: str) -> list[str]:
    return {
        "sm_prepare_input": [],
        "sm_sap_extract": ["SM_INSTALLATIONS.csv"],
        "sm_ingest_final": ["SM_DADOS_FATURA.csv"],
    }[step_name]


def _workflow_step_outputs(step_name: str) -> list[str]:
    return {
        "sm_prepare_input": ["SM_INSTALLATIONS.csv"],
        "sm_sap_extract": ["SM_DADOS_FATURA.csv", "sm_manifest.json", "SM_SQVI1_*.txt", "SM_SQVI2_*.txt"],
        "sm_ingest_final": [],
    }[step_name]


def _normalize_artifact_name(value: str) -> str:
    token = str(value or "").strip().replace("\\", "/")
    if not token or token.startswith("/") or ".." in token.split("/"):
        raise ValueError(f"Invalid artifact name: {value!r}")
    return token
