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
