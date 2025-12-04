"""
Celery Configuration for SolAnalyzer
=====================================
Configures Celery task queue with RabbitMQ broker and Redis backend.
"""

import os
from celery import Celery
from redis import Redis
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get configuration from environment variables with defaults
CELERY_BROKER_URL = os.getenv(
    'CELERY_BROKER_URL', 
    'amqp://guest:guest@rabbitmq:5672//'
)
CELERY_RESULT_BACKEND = os.getenv(
    'CELERY_RESULT_BACKEND', 
    'redis://:prueba@redis:6379/0'
)

# Redis configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', 'prueba')
REDIS_DB = int(os.getenv('REDIS_DB', 0))

# Initialize Celery app
app = Celery(
    'solanalyzer',
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND
)

# Celery configuration
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=600,  # 10 minutes max per task
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)

# Initialize Redis client
redis_client = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=False
)


def get_redis_client():
    """Return the configured Redis client instance."""
    return redis_client


def get_celery_app():
    """Return the configured Celery app instance."""
    return app
