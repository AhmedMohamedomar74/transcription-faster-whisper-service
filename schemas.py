from datetime import datetime
from pydantic import BaseModel
from typing import Optional, List


class Word(BaseModel):
    start: float
    end: float
    word: str
    probability: float


class Segment(BaseModel):
    start: float
    end: float
    text: str
    words: Optional[List[Word]] = None


class TranscriptionResult(BaseModel):
    language: str
    language_probability: float
    duration: float
    segments: List[Segment]


class JobStatus(BaseModel):
    job_id: str
    status: str
    position: Optional[int] = None
    progress: Optional[dict] = None
    estimated_wait_seconds: Optional[int] = None
    result: Optional[TranscriptionResult] = None
    error: Optional[str] = None
    s3_result_key: Optional[str] = None


class TranscriptionRecord(BaseModel):
    job_id: str
    filename: str
    model_size: str
    language: str
    language_probability: float
    audio_duration: float
    vad_removed_seconds: float
    duration_after_vad: float
    processing_time_seconds: float
    created_at: datetime
    s3_result_key: Optional[str] = None
    media_type: Optional[str] = None


class TranscriptionStats(BaseModel):
    total_jobs: int
    total_audio_hours: float
    avg_processing_ratio: float
    by_model: dict
    by_language: dict
