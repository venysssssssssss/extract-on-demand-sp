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
            return self._to_job_envelope(row)

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
