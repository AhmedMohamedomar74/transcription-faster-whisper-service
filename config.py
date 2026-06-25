from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_size: str = "small"
    device: str = "cpu"
    compute_type: str = "int8"
    max_file_size_mb: int = 1024
    redis_url: str = "redis://redis:6379/0"
    upload_dir: str = "/tmp/uploads"
    job_result_ttl_seconds: int = 86400
    mongo_url: str = "mongodb://mongo:27017/whisper"

    # Task time limits
    task_soft_time_limit: int = 3600   # 1 hour — raise if processing very long files
    task_time_limit: int = 3660

    # Audio chunking — files longer than this (seconds) are split into parallel tasks
    chunk_duration_seconds: int = 600   # 10 min per chunk

    # Performance knobs (all overridable via env vars)
    cpu_threads: int = 2    # 2 per worker × 2 workers = 4 cores total
    beam_size: int = 1
    best_of: int = 1
    batch_size: int = 4     # reduced to fit 2 workers in memory

    # S3 / SeaweedFS
    s3_endpoint_url: str = "http://seaweedfs:8333"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "transcriptions"
    s3_region: str = "us-east-1"
    local_processing_dir: str = "/tmp/processing"


settings = Settings()
