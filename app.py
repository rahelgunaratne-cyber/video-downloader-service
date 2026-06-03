"""
ytdlp-service/app.py

FastAPI service: accepts a list of TikTok URLs, resolves each to a direct
CDN mp4 via tiktok-scraper7 (RapidAPI), downloads it, uploads to GCS,
and calls a webhook on completion.

TikTok URLs are resolved via tiktok-scraper7.p.rapidapi.com which returns a
presigned CDN `play` URL — no yt-dlp scraping needed, works from any IP.
Non-TikTok URLs fall back to yt-dlp.

POST /download
  Body: {
    "urls":        ["https://www.tiktok.com/@user/video/123", ...],
    "job_id":      "optional-caller-job-id",
    "webhook_url": "https://...",
    "gcs_prefix":  "tiktok_videos"
  }
  Returns: { "job_id": "...", "status": "running", "video_count": N }

GET /jobs/{job_id}  → full job status with per-video progress and GCS paths
GET /health
"""

import asyncio, json, logging, os, re, subprocess, tempfile, time, uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from google.cloud import storage as gcs_lib

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GCS_BUCKET    = os.environ.get("GCS_BUCKET", "cmg-viz-chat")
MAX_PARALLEL  = int(os.environ.get("MAX_PARALLEL", "4"))
IMPERSONATE   = os.environ.get("YTDLP_IMPERSONATE", "Chrome-131")
RAPIDAPI_KEY  = os.environ.get("RAPIDAPI_KEY") or os.environ.get("RAPID_API_KEY", "")
SCRAPER7_HOST = "tiktok-scraper7.p.rapidapi.com"

app = FastAPI(title="yt-dlp Download Service")
gcs: gcs_lib.Client = None

@app.on_event("startup")
async def startup():
    global gcs
    gcs = gcs_lib.Client()
    log.info("GCS client ready. Bucket: %s", GCS_BUCKET)


# ── Schemas ───────────────────────────────────────────────────────────────────

class DownloadRequest(BaseModel):
    urls:        list[str]
    job_id:      Optional[str] = None          # caller's parent job ID (stored for reference)
    webhook_url: Optional[str] = None          # POST called on completion
    gcs_prefix:  Optional[str] = "tiktok_videos"

class VideoStatus(BaseModel):
    url:       str
    video_id:  Optional[str] = None
    status:    str                             # "pending" | "downloading" | "uploading" | "done" | "error"
    gcs_path:  Optional[str] = None            # gs://bucket/prefix/video_id.mp4
    error:     Optional[str] = None
    elapsed_s: Optional[float] = None

class JobStatus(BaseModel):
    job_id:       str
    parent_job_id: Optional[str] = None
    status:       str                          # "running" | "complete" | "error"
    video_count:  int
    done_count:   int
    error_count:  int
    videos:       list[VideoStatus]
    created_at:   str
    completed_at: Optional[str] = None
    webhook_url:  Optional[str] = None
    gcs_prefix:   str


# ── In-memory job store (backed by GCS for durability) ────────────────────────

jobs: dict[str, JobStatus] = {}


def save_job(job: JobStatus) -> None:
    """Persist job to GCS so it survives instance restarts."""
    try:
        blob = gcs.bucket(GCS_BUCKET).blob(f"ytdlp_jobs/{job.job_id}.json")
        blob.upload_from_string(
            job.model_dump_json(indent=2),
            content_type="application/json",
        )
    except Exception as e:
        log.warning("GCS job save failed: %s", e)


def load_job_from_gcs(job_id: str) -> Optional[JobStatus]:
    try:
        blob = gcs.bucket(GCS_BUCKET).blob(f"ytdlp_jobs/{job_id}.json")
        if not blob.exists():
            return None
        data = json.loads(blob.download_as_text())
        return JobStatus(**data)
    except Exception:
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_video_id(url: str) -> Optional[str]:
    m = re.search(r'/video/(\d+)', url)
    return m.group(1) if m else None


def _resolve_tiktok_cdn(tiktok_url: str) -> str:
    """
    Use tiktok-scraper7 to get a direct presigned CDN mp4 URL.
    Returns the `play` (no-watermark) URL. Raises on failure.
    """
    import urllib.parse, urllib.request
    headers = {
        "x-rapidapi-key":  RAPIDAPI_KEY,
        "x-rapidapi-host": SCRAPER7_HOST,
    }
    api_url = f"https://{SCRAPER7_HOST}/?url={urllib.parse.quote(tiktok_url)}"
    req = urllib.request.Request(api_url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        d = json.loads(resp.read())
    if d.get("code") != 0:
        raise RuntimeError(f"scraper7 error: {d.get('msg', d)}")
    play = d.get("data", {}).get("play") or d.get("data", {}).get("wmplay")
    if not play:
        raise RuntimeError("scraper7 returned no play URL")
    return play


def _download_and_upload(video: VideoStatus, gcs_prefix: str, job_id: str) -> VideoStatus:
    """
    Resolve + download a TikTok video and upload to GCS.
    - TikTok URLs: resolved to CDN mp4 via tiktok-scraper7, downloaded with httpx
    - Other URLs: downloaded with yt-dlp (fallback)
    """
    video_id = video.video_id or extract_video_id(video.url) or uuid.uuid4().hex[:12]
    video.video_id = video_id
    video.status   = "downloading"
    t0 = time.time()

    is_tiktok = "tiktok.com" in video.url

    with tempfile.TemporaryDirectory() as tmpdir:
        mp4_path = Path(tmpdir) / f"{video_id}.mp4"

        try:
            if is_tiktok and RAPIDAPI_KEY:
                # Resolve CDN URL via tiktok-scraper7, then stream-download
                log.info("[%s] Resolving TikTok CDN URL...", video_id)
                cdn_url = _resolve_tiktok_cdn(video.url)
                log.info("[%s] Downloading from CDN...", video_id)
                import urllib.request
                urllib.request.urlretrieve(cdn_url, str(mp4_path))
            else:
                # Fallback: yt-dlp for non-TikTok URLs
                out_tmpl = str(Path(tmpdir) / f"{video_id}.%(ext)s")
                cmd = [
                    "yt-dlp", "--impersonate", IMPERSONATE, "--no-playlist",
                    "-f", "mp4/best[height<=720]", "--merge-output-format", "mp4",
                    "-o", out_tmpl, "--quiet", "--no-warnings", video.url,
                ]
                r = subprocess.run(cmd, capture_output=True, timeout=120)
                if r.returncode != 0:
                    raise RuntimeError(r.stderr.decode(errors="replace").strip()[-300:])
                mp4_files = list(Path(tmpdir).glob("*.mp4"))
                if not mp4_files:
                    raise RuntimeError("yt-dlp produced no mp4 output")
                mp4_path = mp4_files[0]

        except Exception as e:
            log.error("[%s] Download failed: %s", video_id, e)
            video.status    = "error"
            video.error     = str(e)
            video.elapsed_s = round(time.time() - t0, 1)
            return video

        if not mp4_path.exists() or mp4_path.stat().st_size < 1000:
            video.status    = "error"
            video.error     = "Downloaded file is empty or missing"
            video.elapsed_s = round(time.time() - t0, 1)
            return video

        size_kb = mp4_path.stat().st_size // 1024
        log.info("[%s] Downloaded %d KB, uploading to GCS...", video_id, size_kb)
        video.status = "uploading"

        gcs_path = f"{gcs_prefix}/{video_id}.mp4"
        try:
            blob = gcs.bucket(GCS_BUCKET).blob(gcs_path)
            blob.upload_from_filename(str(mp4_path), content_type="video/mp4")
            video.gcs_path  = f"gs://{GCS_BUCKET}/{gcs_path}"
            video.status    = "done"
            log.info("[%s] Uploaded → %s", video_id, video.gcs_path)
        except Exception as e:
            video.status = "error"
            video.error  = f"GCS upload failed: {e}"

    video.elapsed_s = round(time.time() - t0, 1)
    return video


async def run_job(job: JobStatus) -> None:
    """Main async job runner — parallelises downloads, then fires webhook."""
    loop = asyncio.get_event_loop()

    # Update individual video statuses in-place
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        futures = {
            loop.run_in_executor(pool, _download_and_upload, v, job.gcs_prefix, job.job_id): v
            for v in job.videos
        }
        for fut in asyncio.as_completed(futures):
            result: VideoStatus = await fut
            # Patch the matching video entry in job.videos
            for i, v in enumerate(job.videos):
                if v.url == result.url:
                    job.videos[i] = result
                    break
            job.done_count  = sum(1 for v in job.videos if v.status == "done")
            job.error_count = sum(1 for v in job.videos if v.status == "error")
            save_job(job)  # persist incremental progress

    job.status       = "complete" if job.error_count < len(job.videos) else "error"
    job.completed_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    save_job(job)
    log.info("Job %s complete: %d done, %d errors", job.job_id, job.done_count, job.error_count)

    # Fire webhook
    if job.webhook_url:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                await client.post(job.webhook_url, json=job.model_dump())
            log.info("Webhook delivered → %s", job.webhook_url)
        except Exception as e:
            log.warning("Webhook failed: %s", e)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "service": "ytdlp-service", "active_jobs": len(jobs)}


@app.post("/download")
async def download(req: DownloadRequest, background_tasks: BackgroundTasks):
    if not req.urls:
        raise HTTPException(400, "urls list is empty")
    if len(req.urls) > 50:
        raise HTTPException(400, "Max 50 URLs per request")

    job_id  = uuid.uuid4().hex[:16]
    prefix  = (req.gcs_prefix or "tiktok_videos").strip("/")
    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    job = JobStatus(
        job_id        = job_id,
        parent_job_id = req.job_id,
        status        = "running",
        video_count   = len(req.urls),
        done_count    = 0,
        error_count   = 0,
        videos        = [
            VideoStatus(
                url      = url,
                video_id = extract_video_id(url),
                status   = "pending",
            )
            for url in req.urls
        ],
        created_at  = created,
        webhook_url = req.webhook_url,
        gcs_prefix  = prefix,
    )

    jobs[job_id] = job
    save_job(job)
    background_tasks.add_task(run_job, job)

    log.info("Job %s created: %d videos", job_id, len(req.urls))
    return {
        "job_id":      job_id,
        "status":      "running",
        "video_count": len(req.urls),
    }


@app.get("/jobs/{job_id}", response_model=JobStatus)
async def get_job(job_id: str):
    # Check in-memory first, then fall back to GCS
    job = jobs.get(job_id) or load_job_from_gcs(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found")
    return job
