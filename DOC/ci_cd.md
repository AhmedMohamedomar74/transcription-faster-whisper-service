# CI/CD Pipeline for Transcription Service

## Context

The project shifted from a library (published to PyPI) to a self-hosted Docker service. The old CI (`.github/workflows/ci.yml`) targets the deleted library (setup.py, pytest, PyPI). We need a new pipeline that lints the service code, builds a Docker image to GHCR, and deploys via self-hosted runner using `docker-compose.prod.yml` on a shared Docker network.

The service name is **transcription**. GHCR image path will use `${{ github.repository }}` for dynamic owner/repo.

---

## Workflow Structure (4 files)

```
.github/workflows/
├── pipeline.yml   # Orchestrator — triggers on push/PR, calls the others
├── ci.yml         # Reusable — lint + audit
├── build.yml      # Reusable — Docker build + push to GHCR
└── deploy.yml     # Reusable — self-hosted deploy with rollback
```

---

## File 1: `.github/workflows/pipeline.yml`

```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [main]
    paths:
      - "service/**"
      - ".github/workflows/**"
  pull_request:
    branches: [main]
  workflow_dispatch:

jobs:
  ci:
    uses: ./.github/workflows/ci.yml

  build:
    needs: [ci]
    if: github.event_name != 'pull_request'
    uses: ./.github/workflows/build.yml
    permissions:
      packages: write
      contents: read

  deploy:
    needs: [build]
    if: github.event_name != 'pull_request'
    uses: ./.github/workflows/deploy.yml
    with:
      image_tag: ${{ needs.build.outputs.image_tag }}
    secrets: inherit
```

---

## File 2: `.github/workflows/ci.yml`

```yaml
name: CI

on:
  workflow_call:

jobs:
  lint:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
          cache-dependency-path: service/requirements.txt

      - name: Install dependencies
        run: |
          pip install -r service/requirements.txt
          pip install black isort flake8 pip-audit

      - name: Check formatting (black)
        run: black --check service/

      - name: Check import order (isort)
        run: isort --check service/

      - name: Lint (flake8)
        run: flake8 service/

      - name: Security audit
        run: pip-audit -r service/requirements.txt
```

---

## File 3: `.github/workflows/build.yml`

```yaml
name: Build

on:
  workflow_call:
    outputs:
      image_tag:
        description: "The short SHA tag of the built image"
        value: ${{ jobs.build.outputs.image_tag }}

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}/transcription

jobs:
  build:
    runs-on: ubuntu-latest
    timeout-minutes: 20
    permissions:
      packages: write
      contents: read
    outputs:
      image_tag: ${{ steps.meta.outputs.image_tag }}
    steps:
      - uses: actions/checkout@v4

      - uses: docker/setup-buildx-action@v3

      - name: Login to GHCR
        uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Generate image tag
        id: meta
        run: echo "image_tag=${{ github.ref_name }}-${GITHUB_SHA::7}" >> "$GITHUB_OUTPUT"

      - name: Build and push
        uses: docker/build-push-action@v6
        with:
          context: ./service
          file: ./service/Dockerfile
          push: true
          cache-from: type=gha
          cache-to: type=gha,mode=max
          tags: |
            ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:latest
            ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${{ steps.meta.outputs.image_tag }}
```

---

## File 4: `.github/workflows/deploy.yml`

```yaml
name: Deploy

on:
  workflow_call:
    inputs:
      image_tag:
        required: true
        type: string

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}/transcription

jobs:
  deploy:
    runs-on: self-hosted
    timeout-minutes: 10
    steps:
      - name: Login to GHCR
        uses: docker/login-action@v4
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Snapshot current image for rollback
        id: snapshot
        run: |
          cd ${{ secrets.DEPLOY_PATH }}
          CONTAINER_ID=$(docker compose -f docker-compose.prod.yml ps -q whisper-api 2>/dev/null | head -1)
          if [ -n "$CONTAINER_ID" ]; then
            PREV=$(docker inspect --format='{{.Image}}' "$CONTAINER_ID" 2>/dev/null)
            echo "prev_image=$PREV" >> $GITHUB_OUTPUT
            echo "Snapshotted: $PREV"
          else
            echo "prev_image=" >> $GITHUB_OUTPUT
            echo "No running container — first deployment"
          fi

      - name: Write .env and deploy
        env:
          ENV_FILE_CONTENT: ${{ secrets.APP_ENV_FILE }}
        run: |
          cd ${{ secrets.DEPLOY_PATH }}
          printf '%s\n' "$ENV_FILE_CONTENT" > .env

          # Set the image tag in env for docker compose
          echo "IMAGE_TAG=${{ inputs.image_tag }}" >> .env
          echo "REGISTRY=${{ env.REGISTRY }}" >> .env
          echo "IMAGE_NAME=${{ env.IMAGE_NAME }}" >> .env

          docker compose -f docker-compose.prod.yml pull whisper-api whisper-worker
          docker compose -f docker-compose.prod.yml --env-file .env up -d --no-build

      - name: Health check
        run: |
          sleep 15
          for i in $(seq 1 30); do
            HTTP_CODE=$(docker exec $(docker ps -qf "name=whisper-api" | head -1) curl -sf -o /dev/null -w "%{http_code}" http://localhost:8000/health 2>/dev/null || true)
            if [ "$HTTP_CODE" = "200" ]; then
              echo "Health check passed on attempt $i"
              exit 0
            fi
            echo "Attempt $i/30 (HTTP $HTTP_CODE) — waiting 10s..."
            sleep 10
          done
          echo "Health check failed after 300s"
          exit 1

      - name: Rollback on failure
        if: failure()
        run: |
          cd ${{ secrets.DEPLOY_PATH }}
          PREV="${{ steps.snapshot.outputs.prev_image }}"
          if [ -n "$PREV" ]; then
            echo "Rolling back to: $PREV"
            docker tag "$PREV" ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:latest
            docker compose -f docker-compose.prod.yml --env-file .env up -d --no-build whisper-api whisper-worker
          else
            echo "No previous image — cannot rollback"
          fi
```

---

## File Changes

### Delete: `.github/workflows/ci.yml` (old)
The existing ci.yml references deleted library files (setup.py, tests/, PyPI publish). Delete it — the new `ci.yml` above replaces it.

### Update: `service/docker-compose.prod.yml`
Remove `build: .` from whisper-api and whisper-worker, replace with `image:` that pulls from GHCR. Prod **never builds locally** — it always pulls pre-built images.

```yaml
# BEFORE (current):
whisper-api:
  build: .
  ...

# AFTER:
whisper-api:
  image: ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG:-latest}
  ...
```

Same change for `whisper-worker`. The dev `docker-compose.yml` keeps `build: .` unchanged.

### Fix: `service/.flake8`
Change `max-line-length = 88` to `max-line-length = 90` to match `pyproject.toml` black `line-length = 90`.

---

## Required GitHub Secrets

| Secret | Purpose |
|--------|---------|
| `DEPLOY_PATH` | Absolute path on self-hosted runner where `docker-compose.prod.yml` lives |
| `APP_ENV_FILE` | Full `.env` file content (REDIS_URL, MONGO_URL, S3 keys, MODEL_SIZE, etc.) |

`GITHUB_TOKEN` is automatic — no PAT needed for GHCR.

---

## Verification

1. **Local lint:** `cd service && black --check . && isort --check . && flake8 .`
2. **Local build:** `docker build -t transcription:test ./service`
3. **PR test:** push a PR → verify only `ci` job runs (lint + audit)
4. **Merge test:** merge to main → verify full pipeline: ci → build (image appears in GHCR packages) → deploy
5. **Health:** after deploy, `curl <server-ip>/health` returns `{"status": "alive", ...}`
6. **Rollback test:** push a broken commit → verify rollback step triggers and previous image is restored
