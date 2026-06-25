import json
import logging

import redis

from config import settings

logger = logging.getLogger(__name__)

_redis_client = None


def get_redis():
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(settings.redis_url)
    return _redis_client


def publish_job_status(job_id: str, status: str, **extra):
    try:
        r = get_redis()
        data = {"status": status, **extra}
        r.publish(f"job:{job_id}:status", json.dumps(data))
    except Exception as e:
        logger.warning("Redis pub failed (non-fatal): %s", e)


def subscribe_job_status(job_id: str):
    r = get_redis()
    pubsub = r.pubsub()
    pubsub.subscribe(f"job:{job_id}:status")
    return pubsub
