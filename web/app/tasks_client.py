"""
A lightweight Celery client for enqueueing tasks that the worker/celery
services execute (app/tasks.py in ../worker). Only used to send tasks by
name - never imports or runs the worker's task code.
"""
from celery import Celery

from app.config import settings

celery_client = Celery("mediabridge", broker=settings.broker_url)


def enqueue_copy_job(job_id: int) -> str:
    result = celery_client.send_task("copy_media_file", args=[job_id], queue="copy")
    return result.id


def enqueue_verify_job(job_id: int) -> str:
    result = celery_client.send_task("verify_copy_job", args=[job_id], queue="copy")
    return result.id
