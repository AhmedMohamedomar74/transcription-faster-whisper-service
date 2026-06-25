# Faster Whisper Transcription Service — API Documentation

Base URL: `http://localhost:8100`

---

## Flows

### Flow 1 — Simple (single endpoint, for clients)

```
POST /transcribe/async (multipart file) → {job_id}
GET  /jobs/{job_id}/stream              → SSE events until done/failed
```

### Flow 2 — Presigned URL (fastest, for clients)

```
POST /upload/request                    → {job_id, s3_key, presigned_url}
PUT  <presigned_url>  (file binary)     → 200
POST /transcribe      (JSON body)       → {job_id}
GET  /jobs/{job_id}/stream              → SSE events until done/failed
```

### Flow 3 — Service-to-Service (Redis pub/sub)

For backend services that need to react to transcription events without HTTP connections.

```
Your Service                        Transcription Service
    │                                       │
    ├── POST /transcribe/async (file)       │
    │   or POST /transcribe (JSON)          │
    │                                       │
    ├── Subscribe to Redis channel          │
    │   "job:{job_id}:status"               │
    │                                       │
    │              ← Redis pub/sub ←  Worker publishes events
    │   {"status": "downloading"}           │
    │   {"status": "transcribing"}          │
    │   {"status": "done", ...}             │
    └── Unsubscribe                         │
```

**Requirements:** Your service needs direct access to the same Redis instance (`redis://redis:6379/0`).

**Channel format:** `job:{job_id}:status`

**Python example:**
```python
import redis
import json

r = redis.Redis(host="redis", port=6379)
pubsub = r.pubsub()

# After submitting a job and getting job_id
pubsub.subscribe(f"job:{job_id}:status")

for message in pubsub.listen():
    if message["type"] == "message":
        data = json.loads(message["data"])
        print(data["status"])

        if data["status"] == "done":
            duration = data["duration"]
            segments = data["segments"]  # segment count
            # Fetch full result from API or S3
            break

        if data["status"] == "failed":
            error = data["error"]
            break

pubsub.unsubscribe()
pubsub.close()
```

**Node.js example:**
```javascript
import Redis from "ioredis";

const redis = new Redis({ host: "redis", port: 6379 });

// After submitting a job and getting jobId
redis.subscribe(`job:${jobId}:status`);

redis.on("message", (channel, message) => {
  const data = JSON.parse(message);
  console.log(data.status);

  if (data.status === "done" || data.status === "failed") {
    redis.unsubscribe();
    redis.quit();
  }
});
```

**When to use which flow:**

| Flow | Best for | Connection |
|------|----------|------------|
| Flow 1 (Simple) | Browser, mobile, quick testing | HTTP upload + SSE |
| Flow 2 (Presigned) | Large files, high throughput clients | Direct S3 + SSE |
| Flow 3 (Redis) | Backend services, microservices | Redis pub/sub (no HTTP stream) |

---

## Endpoints

### Transcription

#### `POST /transcribe/async`

Upload a file and start transcription. Response time depends on file size (~3-4s).

**Content-Type:** `multipart/form-data`

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file` | file | yes | Audio/video file (mp4, mkv, webm, mp3, ogg, wav, flac, m4a) |
| `language` | string | no | BCP-47 code (`ar`, `en`, `fr`). Null = auto-detect |
| `model_size` | string | no | `tiny`, `base`, `small`, `medium`, `large-v1`, `large-v2`, `large-v3`. Null = server default |

**Response** `200`:
```json
{
  "job_id": "77481289-bd3a-4902-b9f6-e09470bc19cc"
}
```

**Errors:**
- `400` — No file provided or unknown model_size
- `413` — File too large (max 1024MB)

---

#### `POST /upload/request`

Generate a presigned URL for direct upload to S3. Responds in ~2ms.

**Content-Type:** `application/json`

**Request:**
```json
{
  "filename": "recording.mp4"
}
```

**Response** `200`:
```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "s3_key": "3fa85f64-5717-4562-b3fc-2c963f66afa6/original.mp4",
  "presigned_url": "http://localhost:8333/transcriptions/3fa85f64-.../original.mp4?X-Amz-Signature=...",
  "expires_in": 3600
}
```

After receiving the URL, upload the file directly:
```bash
curl -X PUT "<presigned_url>" --upload-file recording.mp4
```

---

#### `POST /transcribe`

Start transcription for a file already uploaded to S3. Responds in ~2ms.

**Content-Type:** `application/json`

**Request:**
```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "s3_key": "3fa85f64-5717-4562-b3fc-2c963f66afa6/original.mp4",
  "filename": "recording.mp4",
  "language": null,
  "model_size": null
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `job_id` | string | yes | From `/upload/request` |
| `s3_key` | string | yes | From `/upload/request` |
| `filename` | string | yes | Original filename (for metadata) |
| `language` | string | no | BCP-47 code. Null = auto-detect |
| `model_size` | string | no | Null = server default |

**Response** `200`:
```json
{
  "job_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6"
}
```

---

### Job Status

#### `GET /jobs/{job_id}`

Poll job status. Returns current state.

**Response** `200`:

```json
// pending
{
  "job_id": "...",
  "status": "pending",
  "estimated_wait_seconds": 120
}

// running
{
  "job_id": "...",
  "status": "running",
  "progress": {"chunks": 6, "status": "transcribing_chunks"}
}

// done
{
  "job_id": "...",
  "status": "done",
  "s3_result_key": "{job_id}/transcription.json",
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
          {"start": 0.0, "end": 1.2, "word": "مرحباً", "probability": 0.99}
        ]
      }
    ]
  }
}

// failed
{
  "job_id": "...",
  "status": "failed",
  "error": "CUDA out of memory"
}
```

| Status | Meaning |
|--------|---------|
| `pending` | Queued, not started |
| `running` | Worker is processing |
| `done` | Complete — `result` populated |
| `failed` | Error — `error` populated. Also persisted in S3 (`{job_id}/error.json`) and MongoDB |

---

#### `GET /jobs/{job_id}/stream`

SSE (Server-Sent Events) stream for real-time job status.

**Headers:** `Accept: text/event-stream`

**Connection behavior:**
- If job is already done/failed, returns final event immediately and closes
- Otherwise, streams events as the worker progresses
- Sends `ping` events as keepalive every 5 seconds
- Stream closes automatically after `done` or `failed`

**Example with curl:**
```bash
curl -N http://localhost:8100/jobs/{job_id}/stream
```

**Example with JavaScript:**
```javascript
const es = new EventSource("http://localhost:8100/jobs/{job_id}/stream");

es.addEventListener("status", (e) => {
  const data = JSON.parse(e.data);
  console.log(data.status, data);

  if (data.status === "done" || data.status === "failed") {
    es.close();
  }
});
```

**Example with Python:**
```python
import requests
import json

response = requests.get(
    f"http://localhost:8100/jobs/{job_id}/stream",
    stream=True,
    headers={"Accept": "text/event-stream"},
)

for line in response.iter_lines():
    line = line.decode()
    if line.startswith("data:"):
        data = json.loads(line[5:].strip())
        print(data["status"], data)
        if data["status"] in ("done", "failed"):
            break
```

---

## Events

These events are published by the worker via Redis pub/sub to channel `job:{job_id}:status`. They are the same events delivered through SSE (`GET /jobs/{job_id}/stream`) and Redis pub/sub (Flow 3).

All events are JSON objects with a `status` field.

### Event Flow

```
downloading → analyzing → compressing → splitting → transcribing → done
                                                                  → failed (at any step)
                                                    → merging → done (for chunked audio)
```

### Event Reference

| Event | Fields | Description |
|-------|--------|-------------|
| `downloading` | `{"status": "downloading"}` | Downloading file from S3 |
| `analyzing` | `{"status": "analyzing"}` | Running ffprobe to detect media type |
| `compressing` | `{"status": "compressing"}` | Converting with ffmpeg (video/large files) |
| `splitting` | `{"status": "splitting"}` | Splitting long audio into chunks |
| `transcribing` | `{"status": "transcribing", "chunks": 1}` | Running Whisper. `chunks` = number of audio chunks |
| `merging` | `{"status": "merging"}` | Merging chunk results (only for multi-chunk jobs) |
| `done` | `{"status": "done", "duration": 47.32, "segments": 12}` | Transcription complete |
| `failed` | `{"status": "failed", "error": "..."}` | Error occurred |
| `ping` | `""` | Keepalive (every 5s) |

### Raw SSE format

```
event: status
data: {"status": "downloading"}

event: status
data: {"status": "analyzing"}

event: status
data: {"status": "compressing"}

event: status
data: {"status": "splitting"}

event: status
data: {"status": "transcribing", "chunks": 1}

event: status
data: {"status": "done", "duration": 47.32, "segments": 12}
```

---

## Transcription History

#### `GET /transcriptions`

List completed transcriptions (paginated, no segments).

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `limit` | int | 20 | Max items (capped at 100) |
| `skip` | int | 0 | Offset |
| `language` | string | — | Filter by language |
| `model_size` | string | — | Filter by model |

**Response** `200`:
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
      "media_type": "audio",
      "language": "ar",
      "language_probability": 0.99,
      "audio_duration": 460.1,
      "vad_removed_seconds": 16.1,
      "duration_after_vad": 444.0,
      "processing_time_seconds": 85.3,
      "s3_result_key": "abc-123/transcription.json",
      "chunks": 1,
      "created_at": "2026-06-25T20:25:00Z"
    }
  ]
}
```

---

#### `GET /transcriptions/stats`

Aggregated statistics.

**Response** `200`:
```json
{
  "total_jobs": 42,
  "total_audio_hours": 5.23,
  "avg_processing_ratio": 0.45,
  "by_model": {"small": 30, "base": 12},
  "by_language": {"ar": 25, "en": 17}
}
```

---

#### `GET /transcriptions/compare?job_ids=id1,id2`

Compare multiple jobs side by side.

---

#### `GET /transcriptions/{job_id}`

Full transcription record including segments.

---

#### `GET /transcriptions/{job_id}/segments`

Segments only.

**Response** `200`:
```json
{
  "job_id": "abc-123",
  "segments": [
    {
      "start": 0.0,
      "end": 4.56,
      "text": "مرحباً بالعالم",
      "words": [
        {"start": 0.0, "end": 1.2, "word": "مرحباً", "probability": 0.99},
        {"start": 1.3, "end": 2.1, "word": "بالعالم", "probability": 0.97}
      ]
    }
  ]
}
```

---

## Health & Readiness

#### `GET /health`

```json
{
  "status": "alive",
  "model_loaded": true,
  "queue_depth": 0
}
```

#### `GET /ready`

Returns `200` if worker is available, `503` if not.

```json
{"status": "ready"}
// or
{"status": "not_ready", "reason": "worker_not_available"}
```

#### `GET /models`

```json
{
  "available": ["tiny", "base", "small", "medium", "large-v1", "large-v2", "large-v3"],
  "default": "small",
  "loaded": ["small"]
}
```

---

## S3 Storage Structure

Each job stores files under `s3://transcriptions/{job_id}/`:

| Key | When | Content |
|-----|------|---------|
| `{job_id}/original.{ext}` | Always | Original uploaded file |
| `{job_id}/transcription.json` | On success | Full transcription result |
| `{job_id}/error.json` | On failure | Error report with traceback |

---

## Data Types

### Segment

```json
{
  "start": 0.0,
  "end": 4.56,
  "text": "مرحباً بالعالم",
  "words": [
    {"start": 0.0, "end": 1.2, "word": "مرحباً", "probability": 0.99}
  ]
}
```

### Word

```json
{
  "start": 0.0,
  "end": 1.2,
  "word": "مرحباً",
  "probability": 0.99
}
```

### TranscriptionResult

```json
{
  "language": "ar",
  "language_probability": 0.998,
  "duration": 47.32,
  "segments": [...]
}
```

---

## Error Handling

| HTTP Code | Meaning |
|-----------|---------|
| 200 | Success |
| 400 | Bad request (missing file, unknown model) |
| 404 | Transcription not found |
| 413 | File too large |
| 503 | Worker not available (readiness check) |

Failed jobs are persisted in:
1. **S3**: `{job_id}/error.json` — includes `error`, `traceback`, `filename`, `model_size`, `failed_at`
2. **MongoDB**: document with `error` field — survives Celery result expiry (24h)
3. **Celery**: task state FAILURE — expires after `JOB_RESULT_TTL_SECONDS` (default 24h)

---

## Configuration (Environment Variables)

| Variable | Default | Description |
|----------|---------|-------------|
| `MODEL_SIZE` | `small` | Default Whisper model |
| `DEVICE` | `cpu` | `cpu` or `cuda` |
| `COMPUTE_TYPE` | `int8` | Model precision |
| `MAX_FILE_SIZE_MB` | `1024` | Max upload size |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection |
| `MONGO_URL` | `mongodb://mongo:27017/whisper` | MongoDB connection |
| `S3_ENDPOINT_URL` | `http://seaweedfs:8333` | S3 internal endpoint |
| `S3_PUBLIC_ENDPOINT_URL` | `http://localhost:8333` | S3 public endpoint (for presigned URLs) |
| `S3_BUCKET` | `transcriptions` | S3 bucket name |
| `PRESIGNED_URL_EXPIRATION_SECONDS` | `3600` | Presigned URL TTL |
| `CHUNK_DURATION_SECONDS` | `600` | Audio split threshold (seconds) |
| `BATCH_SIZE` | `4` | Whisper batch size |
| `BEAM_SIZE` | `1` | Beam search width |
| `CPU_THREADS` | `2` | Threads per worker |
| `TASK_SOFT_TIME_LIMIT` | `3600` | Soft time limit (seconds) |
| `TASK_TIME_LIMIT` | `3660` | Hard time limit (seconds) |
| `JOB_RESULT_TTL_SECONDS` | `86400` | Celery result expiry |
