# Faster Whisper Transcription Service — API Documentation

## Overview

An async audio transcription REST API powered by [faster-whisper](https://github.com/SYSTRAN/faster-whisper).
Jobs are queued via Celery + Redis and processed by a background worker. The API never blocks on transcription — it returns a `job_id` immediately and lets you poll for results.

**Base URL:** `http://localhost:8000`

---

## Architecture

```
Client  ──POST /transcribe/async──►  FastAPI (api)
                                          │ saves file, enqueues task
                                          ▼
                                      Redis (broker + result backend)
                                          │
                                      Celery Worker
                                          │ transcribes
                                          ├──► MongoDB (saves result permanently)
                                          ▼
                                      Redis (stores result for 24h)

Client  ──GET /jobs/{job_id}──────►  FastAPI ──► Redis (reads result)
```

**Long file flow (> `CHUNK_DURATION_SECONDS`):**

```
  transcribe_audio
      │ split_audio() via ffmpeg
      ├── chunk_0.wav ──► transcribe_audio_chunk (Worker 1)
      ├── chunk_1.wav ──► transcribe_audio_chunk (Worker 2)  ← parallel
      ├── chunk_2.wav ──► transcribe_audio_chunk (Worker 1)
      └── ...
            │ all chunks done
            ▼
      merge_transcriptions (applies offsets, deduplicates overlap)
            │
            ▼
      Redis (final result under original job_id)
      MongoDB (permanent record with chunks count)
```

---

## Endpoints

### 1. `POST /transcribe/async` — Submit a transcription job

Uploads an audio file and returns a job ID. Processing happens asynchronously in the worker.

> **Long file handling:** Files longer than `CHUNK_DURATION_SECONDS` (default: 600s / 10 min) are automatically split into parallel chunks. The `job_id` returned is still the polling ID — no client-side change required.

**Request**

| Part | Type | Required | Description |
|------|------|----------|-------------|
| `file` | `multipart/form-data` file | Yes | Audio file (any format ffmpeg supports: `.mp3`, `.ogg`, `.wav`, `.flac`, `.m4a`, …) |
| `language` | `form` string | No | BCP-47 language code (e.g. `ar`, `en`, `fr`). Omit for auto-detection. |
| `model_size` | `form` string | No | Whisper model to use (e.g. `base`, `medium`). Defaults to `MODEL_SIZE` env var. |

**Constraints**
- Max file size: `MAX_FILE_SIZE_MB` (default **200 MB**)

**Response `200 OK`**

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
}
```

**Error responses**

| Status | Reason |
|--------|--------|
| `400` | No filename provided |
| `400` | Unknown `model_size` value |
| `413` | File exceeds the size limit |

---

### 2. `GET /jobs/{job_id}` — Poll job status

Returns the current state of a transcription job.

**Path parameter**

| Name | Type | Description |
|------|------|-------------|
| `job_id` | string (UUID) | The ID returned by `POST /transcribe/async` |

**Response `200 OK`** — shape varies by `status`

#### `pending` — job is queued, not yet started

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "pending",
  "estimated_wait_seconds": 120
}
```

#### `running` — worker is actively transcribing

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "running",
  "progress": { "chunks": 6, "status": "transcribing_chunks" }
}
```

#### `done` — transcription complete

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "done",
  "result": {
    "language": "ar",
    "language_probability": 0.998,
    "duration": 47.32,
    "segments": [
      {
        "start": 0.0,
        "end": 4.56,
        "text": "مرحباً بالعالم",
        "words": [
          { "start": 0.0,  "end": 1.2,  "word": "مرحباً", "probability": 0.99 },
          { "start": 1.3,  "end": 2.1,  "word": "بالعالم", "probability": 0.97 }
        ]
      }
    ]
  }
}
```

#### `failed` — transcription encountered an error

```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "failed",
  "error": "CUDA out of memory"
}
```

**`JobStatus` schema**

| Field | Type | Always present | Description |
|-------|------|---------------|-------------|
| `job_id` | string | Yes | UUID of the job |
| `status` | string | Yes | `pending` / `running` / `done` / `failed` |
| `position` | integer | No | Queue position (reserved for future use) |
| `progress` | object | No | Partial progress info while `running` |
| `estimated_wait_seconds` | integer | No | Rough ETA when `pending` |
| `result` | TranscriptionResult | No | Present only when `done` |
| `error` | string | No | Error message when `failed` |

**`TranscriptionResult` schema**

| Field | Type | Description |
|-------|------|-------------|
| `language` | string | Detected or forced language code |
| `language_probability` | float | Confidence of language detection (0–1) |
| `duration` | float | Audio duration in seconds (end of last segment) |
| `segments` | Segment[] | Ordered list of transcript segments |

**`Segment` schema**

| Field | Type | Description |
|-------|------|-------------|
| `start` | float | Start time in seconds |
| `end` | float | End time in seconds |
| `text` | string | Transcribed text for this segment |
| `words` | Word[] | Word-level timestamps (always present) |

**`Word` schema**

| Field | Type | Description |
|-------|------|-------------|
| `start` | float | Word start time in seconds |
| `end` | float | Word end time in seconds |
| `word` | string | The word token |
| `probability` | float | Token confidence (0–1) |

---

### 3. `GET /health` — Liveness check

Returns service liveness and basic metrics. Always returns `200`.

**Response `200 OK`**

```json
{
  "status": "alive",
  "model_loaded": true,
  "queue_depth": 2
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Always `"alive"` |
| `model_loaded` | boolean | `true` if at least one Celery worker responded to ping |
| `queue_depth` | integer | Number of active + reserved tasks across all workers |

---

### 4. `GET /ready` — Readiness check

Returns `200` only when a Celery worker is reachable. Use this as the Kubernetes/Docker readiness probe.

**Response `200 OK`**

```json
{ "status": "ready" }
```

**Response `503 Service Unavailable`** (worker not reachable)

```json
{ "status": "not_ready", "reason": "worker_not_available" }
```

---

### 5. `GET /models` — List available models

Returns all Whisper model sizes supported by faster-whisper and the currently loaded one.

**Response `200 OK`**

```json
{
  "available": ["tiny", "tiny.en", "base", "base.en", "small", "small.en", "medium", "medium.en", "large-v1", "large-v2", "large-v3"],
  "default": "small",
  "loaded": ["small", "base"]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `available` | string[] | All model sizes faster-whisper knows about |
| `default` | string | The default model size (`MODEL_SIZE` env var) |
| `loaded` | string[] | Model sizes currently loaded in the worker cache |

---

### 6. `GET /transcriptions` — List transcription records

Returns a paginated list of past transcription jobs from MongoDB, sorted newest first.

**Query parameters**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `limit` | integer | `20` | Max items (capped at 100) |
| `skip` | integer | `0` | Items to skip for pagination |
| `language` | string | — | Filter by language code (e.g. `ar`) |
| `model_size` | string | — | Filter by model size (e.g. `base`) |

**Response `200 OK`**

```json
{
  "total": 42,
  "skip": 0,
  "limit": 20,
  "items": [
    {
      "job_id": "abc-123",
      "filename": "recording.mp3",
      "model_size": "small",
      "language": "ar",
      "language_probability": 0.998,
      "audio_duration": 47.32,
      "vad_removed_seconds": 3.12,
      "duration_after_vad": 44.2,
      "processing_time_seconds": 12.45,
      "created_at": "2025-01-01T00:00:00Z"
    }
  ]
}
```

**Note:** The `segments` field is excluded from list results for performance. Use `GET /transcriptions/{job_id}` to retrieve segments.

---

### 7. `GET /transcriptions/stats` — Aggregated metrics

Returns summary statistics across all transcription jobs.

**Response `200 OK`**

```json
{
  "total_jobs": 42,
  "total_audio_hours": 5.23,
  "avg_processing_ratio": 0.45,
  "by_model": { "small": 30, "base": 12 },
  "by_language": { "ar": 25, "en": 17 }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `total_jobs` | integer | Total number of transcriptions in MongoDB |
| `total_audio_hours` | float | Sum of all audio durations in hours |
| `avg_processing_ratio` | float | Total processing time / total audio (lower is faster) |
| `by_model` | object | Job count per model size |
| `by_language` | object | Job count per language |

---

### 8. `GET /transcriptions/compare` — Side-by-side model comparison

Compare two or more transcription jobs (e.g. same audio with different models).

**Query parameters**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `job_ids` | string | Yes | Comma-separated job IDs (e.g. `id1,id2`) |

**Response `200 OK`**

```json
{
  "jobs": [
    {
      "job_id": "abc-123",
      "filename": "recording.mp3",
      "model_size": "small",
      "language": "ar",
      "language_probability": 0.998,
      "audio_duration": 47.32,
      "vad_removed_seconds": 3.12,
      "duration_after_vad": 44.2,
      "processing_time_seconds": 12.45,
      "created_at": "2025-01-01T00:00:00Z"
    }
  ]
}
```

---

### 9. `GET /transcriptions/{job_id}` — Full transcription record

Returns the complete transcription record including segments.

**Path parameter**

| Name | Type | Description |
|------|------|-------------|
| `job_id` | string (UUID) | The ID returned by `POST /transcribe/async` |

**Response `200 OK`**

```json
{
  "job_id": "abc-123",
  "filename": "recording.mp3",
  "model_size": "small",
  "language": "ar",
  "language_probability": 0.998,
  "audio_duration": 47.32,
  "vad_removed_seconds": 3.12,
  "duration_after_vad": 44.2,
  "processing_time_seconds": 12.45,
  "segments": [
    {
      "start": 0.0,
      "end": 4.56,
      "text": "مرحباً بالعالم",
      "words": [
        { "start": 0.0, "end": 1.2, "word": "مرحباً", "probability": 0.99 }
      ]
    }
  ],
  "created_at": "2025-01-01T00:00:00Z"
}
```

**Response `404 Not Found`** — invalid `job_id`

```json
{ "detail": "Transcription not found" }
```

---

### 10. `GET /transcriptions/{job_id}/segments` — Segments only

Returns only the segments array for a completed transcription.

**Response `200 OK`**

```json
{
  "job_id": "abc-123",
  "segments": [
    {
      "start": 0.0,
      "end": 4.56,
      "text": "مرحباً بالعالم",
      "words": [
        { "start": 0.0, "end": 1.2, "word": "مرحباً", "probability": 0.99 }
      ]
    }
  ]
}
```

---

### `TranscriptionRecord` schema

| Field | Type | Description |
|-------|------|-------------|
| `job_id` | string | UUID of the job |
| `filename` | string | Original uploaded filename |
| `model_size` | string | Whisper model used |
| `language` | string | Detected or forced language |
| `language_probability` | float | Language detection confidence (0–1) |
| `audio_duration` | float | Total audio duration in seconds |
| `vad_removed_seconds` | float | Seconds removed by VAD (silence/non-speech) |
| `duration_after_vad` | float | Audio duration after VAD filtering |
| `processing_time_seconds` | float | Wall-clock time to transcribe |
| `segments` | Segment[] | Transcribed segments (excluded in list view) |
| `chunks` | integer | Number of audio chunks the file was split into (1 = no splitting) |
| `created_at` | datetime | UTC timestamp when the job completed |

---

## Typical usage flow

```
1.  POST /transcribe/async  (upload file)
        ↓ { "job_id": "abc-123" }

2.  GET  /jobs/abc-123      (poll every 2–5 s)
        ↓ { "status": "pending", ... }
        ↓ { "status": "running", ... }
        ↓ { "status": "done", "result": { ... } }   ← stop polling
```

Results are stored permanently in MongoDB and available for 24 hours in Redis (`JOB_RESULT_TTL_SECONDS`).

---

## Configuration (environment variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `MODEL_SIZE` | `small` | Whisper model variant |
| `DEVICE` | `cpu` | `cpu` or `cuda` |
| `COMPUTE_TYPE` | `int8` | `int8`, `float16`, `float32` |
| `MAX_FILE_SIZE_MB` | `200` | Maximum upload size |
| `REDIS_URL` | `redis://redis:6379/0` | Celery broker + backend |
| `UPLOAD_DIR` | `/tmp/uploads` | Temporary file storage |
| `JOB_RESULT_TTL_SECONDS` | `86400` | How long results are kept in Redis |
| `MONGO_URL` | `mongodb://mongo:27017/whisper` | MongoDB connection string for permanent storage |
| `CHUNK_DURATION_SECONDS` | `600` | Files longer than this are split into parallel chunks. Lower = more parallelism, higher = fewer chunks. |

---

## Running locally

```bash
cp service/.env.example service/.env
docker compose -f service/docker-compose.yml up --build
```

The API will be available at `http://localhost:8000`.  
Interactive Swagger UI: `http://localhost:8000/docs`
