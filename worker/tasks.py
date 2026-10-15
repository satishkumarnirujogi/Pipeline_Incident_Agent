"""Background jobs: one per incident and one per resume-after-approval.

acks_late + reject_on_worker_lost (celery_app.py): a job is acknowledged only when
it finishes, so if the worker dies mid-run the job is redelivered and the runner
continues from the last checkpoint.
"""

from __future__ import annotations

import logging

from worker import runner
from worker.celery_app import celery_app

log = logging.getLogger("worker")


@celery_app.task(name="ping")
def ping() -> str:
    return "pong"


@celery_app.task(name="run_incident")
def run_incident(incident_id: str) -> str:
    return runner.run_incident(incident_id)


@celery_app.task(name="resume_incident")
def resume_incident(incident_id: str, approval_id: str) -> str:
    return runner.resume_incident(incident_id, approval_id)


@celery_app.task(name="expire_stale_approvals")
def expire_stale_approvals() -> int:
    expired = runner.expire_stale_approvals()
    for incident_id, approval_id in expired:
        resume_incident.delay(incident_id, approval_id)
    return len(expired)
