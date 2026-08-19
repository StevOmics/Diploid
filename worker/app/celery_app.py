from celery import Celery

from app.config import settings

app = Celery("mediabridge", broker=settings.broker_url, backend="rpc://", include=["app.tasks"])
app.conf.task_routes = {
    "copy_media_file": {"queue": "copy"},
    "verify_copy_job": {"queue": "copy"},
    "ping": {"queue": "default"},
}
