"""Background jobs. run_incident and resume_incident arrive in Phase 6."""

from worker.celery_app import celery_app


@celery_app.task(name="ping")
def ping() -> str:
    return "pong"
