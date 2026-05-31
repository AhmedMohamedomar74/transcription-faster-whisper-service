import os
import logging

from celery import Celery
from celery.signals import worker_process_init
from faster_whisper import WhisperModel, BatchedInferencePipeline

from config import settings

logger = logging.getLogger(__name__)

celery_app = Celery(
    "transcriber",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    worker_concurrency=1,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=600,
    task_time_limit=660,
    worker_max_tasks_per_child=50,
    task_track_started=True,
    result_expires=settings.job_result_ttl_seconds,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)

@worker_process_init.connect
def preload_model_on_startup(**kwargs):
    get_model()


_model = None


def get_model():
    global _model
    if _model is None:
        logger.info(
            "Loading model: %s (device=%s, compute_type=%s, cpu_threads=%d)",
            settings.model_size,
            settings.device,
            settings.compute_type,
            settings.cpu_threads,
        )
        whisper_model = WhisperModel(
            settings.model_size,
            device=settings.device,
            compute_type=settings.compute_type,
            cpu_threads=settings.cpu_threads,
        )
        _model = BatchedInferencePipeline(model=whisper_model)
        logger.info("Model loaded successfully (batched pipeline, batch_size=%d)", settings.batch_size)
    return _model


@celery_app.task(bind=True, name="transcribe_audio")
def transcribe_audio(self, audio_path: str, language: str = None):
    model = get_model()

    logger.info("Starting transcription: %s (language=%s)", audio_path, language)

    segments, info = model.transcribe(
        audio_path,
        language=language,
        beam_size=settings.beam_size,
        best_of=settings.best_of,
        vad_filter=True,
        vad_parameters={
            "min_silence_duration_ms": 500,
            "speech_pad_ms": 200,
        },
        word_timestamps=True,
        batch_size=settings.batch_size,
    )

    total_duration = 0.0
    segments_list = []
    for segment in segments:
        words = []
        if segment.words:
            for word in segment.words:
                words.append({
                    "start": word.start,
                    "end": word.end,
                    "word": word.word,
                    "probability": word.probability,
                })
        segments_list.append({
            "start": segment.start,
            "end": segment.end,
            "text": segment.text,
            "words": words,
        })
        total_duration = segment.end

    if os.path.isfile(audio_path):
        os.remove(audio_path)

    result = {
        "language": info.language,
        "language_probability": info.language_probability,
        "duration": total_duration,
        "segments": segments_list,
    }

    logger.info(
        "Transcription complete: %s (%d segments, %.1fs)",
        audio_path,
        len(segments_list),
        total_duration,
    )
    return result
