from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_size: str = "small"
    device: str = "cpu"
    compute_type: str = "int8"
    max_file_size_mb: int = 200
    redis_url: str = "redis://redis:6379/0"
    upload_dir: str = "/tmp/uploads"
    job_result_ttl_seconds: int = 86400

    # Performance knobs (all overridable via env vars)
    cpu_threads: int = 4
    beam_size: int = 1
    best_of: int = 1
    batch_size: int = 8


settings = Settings()
