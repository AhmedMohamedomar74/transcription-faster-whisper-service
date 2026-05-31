# Performance Optimization — Faster-Whisper Transcription Service

## What Changed and Why

### Fix 1 — Cold-Start Model Preload

| Before | After |
|--------|-------|
| Model downloads from HuggingFace on first `transcribe_audio()` call | Model loads at worker startup via `worker_process_init` signal |
| First job waited ~2.5 min for download | Zero wait — model is ready before any job arrives |

**How:** A Celery `worker_process_init` signal handler calls `get_model()` as soon as each worker fork initializes.

### Fix 2 — BatchedInferencePipeline (Parallel Chunk Decoding)

| Before | After |
|--------|-------|
| `WhisperModel.transcribe()` decodes 30s chunks serially (1 core) | `BatchedInferencePipeline` stacks up to `batch_size` chunks into one tensor; all cores work in parallel |
| 8-min audio ≈ 16 serial chunks → ~8 min | 8-min audio ≈ 2–4 batches → ~1.5–2 min |

**How:** The global `_model` is now a `BatchedInferencePipeline` wrapping a `WhisperModel`. The pipeline collects chunks until `batch_size` is reached, then calls CTranslate2's batched `generate()`.

### Fix 3 — Greedy Decoding (beam_size=1)

| Before | After |
|--------|-------|
| `beam_size=5, best_of=5` (default) — 5 candidate hypotheses per chunk | `beam_size=1, best_of=1` — single best hypothesis |
| ~5× compute per chunk for marginal accuracy gain | ~5× faster decoding on clear speech |

**How:** `beam_size` and `best_of` are now configurable via env vars and default to 1.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│             Celery Worker Process            │
│                                              │
│  ┌──────────┐    ┌──────────────────────┐    │
│  │  Celery   │    │ BatchedInferencePipe │    │
│  │  Task     │───▶│        line          │    │
│  │  (job)    │    │                      │    │
│  └──────────┘    │  ┌──────────────┐    │    │
│                  │  │ WhisperModel  │    │    │
│                  │  │ (CTranslate2) │    │    │
│                  │  └──────────────┘    │    │
│                  └──────────────────────┘    │
│                       │                     │
│                       ▼                     │
│              ┌──────────────────┐           │
│              │ 4 CPU cores      │           │
│              │ (OpenMP threads) │           │
│              └──────────────────┘           │
└─────────────────────────────────────────────┘
```

**Data flow:**
1. Celery receives a task and calls `transcribe_audio()`
2. Audio is loaded and VAD-split into speech segments (VAD filter)
3. Segments are grouped into batches of `batch_size`
4. Each batch is padded to equal-length tensors and fed to `model.generate()`
5. CTranslate2's OpenMP thread pool distributes the batch across available CPU cores
6. Decoded text with word-level timestamps is returned

---

## Tuning Guide

| Env Var | Default | When to Raise | When to Lower |
|---------|---------|---------------|---------------|
| `CPU_THREADS` | 4 | More physical cores available (set to core count) | Memory contention or hyperthreading causes slowdown |
| `BEAM_SIZE` | 1 | Audio is noisy, accented, or background noise present (3–5) | Clear speech; greedy is sufficient (keep 1) |
| `BEST_OF` | 1 | Always keep `>= BEAM_SIZE`. Only matters when `BEAM_SIZE > 1` | — |
| `BATCH_SIZE` | 8 | Model is small, RAM is plentiful (try 12–16) | OOM errors or using `large-v3` with `float32` (try 4) |

**Quick-tune for throughput** (clear speech, batch audio):
```
CPU_THREADS=$(nproc)
BEAM_SIZE=1
BATCH_SIZE=8
```

**Quick-tune for accuracy** (noisy audio, single files):
```
CPU_THREADS=$(nproc)
BEAM_SIZE=5
BEST_OF=5
BATCH_SIZE=4
```

---

## Trade-offs: Speed vs Accuracy

| beam_size | Relative Speed | Accuracy (WER) | Use Case |
|-----------|---------------|----------------|----------|
| 1 (greedy) | 1.0× (baseline) | Baseline | Clear speech, podcasts, meetings |
| 3 | ~0.5× | ~2–5% better | Moderate noise, accented speech |
| 5 | ~0.3× | ~3–8% better | Heavy noise, music, low-quality recordings |

Notes:
- WER improvement varies by language and audio quality. Test with your data.
- `best_of` only matters during sampling (temperature > 0). With `beam_size=1` it has no effect.
- Greedy decoding is deterministic; beam search may produce different results across runs.

---

## GPU Upgrade Path

To switch from CPU to GPU inference:

1. **Install CUDA dependencies** (in Dockerfile):
   ```dockerfile
   RUN pip install nvidia-cublas-cu11 nvidia-cudnn-cu11
   ```

2. **Set env vars:**
   ```env
   DEVICE=cuda
   COMPUTE_TYPE=float16
   CPU_THREADS=0
   BATCH_SIZE=16
   ```

3. **Why these changes:**
   - `DEVICE=cuda` — run on GPU instead of CPU
   - `COMPUTE_TYPE=float16` — half-precision is 2× faster on GPUs with negligible quality loss
   - `CPU_THREADS=0` — CTranslate2 auto-detects; GPU inference doesn't need OMP tuning
   - `BATCH_SIZE=16` — GPUs handle larger batches; raises throughput

4. **Verify:**
   ```
   # Worker logs should show:
   Loading model: small (device=cuda, compute_type=float16, cpu_threads=0)
   Model loaded successfully (batched pipeline, batch_size=16)
   ```

5. **Expected speedup over CPU:**
   - small model: ~10–15× realtime
   - medium model: ~5–10× realtime
   - large-v3: ~2–4× realtime (with `BATCH_SIZE=4`)

---

## Files Modified

| File | Change |
|------|--------|
| `service/worker.py` | Added `worker_process_init` signal for model preload; switched to `BatchedInferencePipeline`; added `beam_size`/`best_of`/`batch_size` to transcribe call; removed `max_speech_duration_s` |
| `service/config.py` | Added `cpu_threads`, `beam_size`, `best_of`, `batch_size` fields |
| `service/.env.example` | Documented 4 new performance env vars |
| `service/DOC/PERFORMANCE_OPTIMIZATION.md` | This file |
