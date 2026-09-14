from celery import Celery

from app.config import settings

app = Celery("mediabridge", broker=settings.broker_url, backend="rpc://", include=["app.tasks"])
app.conf.task_routes = {
    "copy_media_file": {"queue": "copy"},
    "backup_clump": {"queue": "copy"},
    "verify_copy_job": {"queue": "copy"},
    "ping": {"queue": "default"},
    "retry_failed_transfers": {"queue": "default"},
}
# Runs on the celery-beat service. Polls every 5 minutes rather than hourly so
# each job's own next_retry_at (set to failure time + the configured retry
# interval) is honored to within a few minutes instead of up to an hour late.
app.conf.beat_schedule = {
    "retry-failed-transfers": {
        "task": "retry_failed_transfers",
        "schedule": 300.0,
    },
}
