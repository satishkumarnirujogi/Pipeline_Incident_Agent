"""Celery app. One job per incident and per resume-after-approval."""

from celery import Celery
from celery.signals import setup_logging

from agent import logs
from agent.settings import get_settings

_s = get_settings()

celery_app = Celery("pia", broker=_s.redis_url, backend=_s.redis_url, include=["worker.tasks"])
celery_app.conf.update(
    task_acks_late=True,  # redeliver if the worker dies mid-run
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    # Well above the longest incident run; approval waits do not hold a job.
    broker_transport_options={"visibility_timeout": 900},
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    # Celery beat (embedded in the single worker with -B): escalate stale approvals.
    beat_schedule={
        "expire-stale-approvals": {"task": "expire_stale_approvals", "schedule": 900.0},
    },
)


@setup_logging.connect
def _json_logs(**_: object) -> None:
    """Connecting this signal stops Celery from installing its own log format."""
    logs.configure()
