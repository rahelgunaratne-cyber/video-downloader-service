FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# ffmpeg required by yt-dlp for muxing; curl_cffi for TikTok impersonation
RUN apt-get update && apt-get install -y --no-install-recommends \
      ffmpeg \
      curl \
      ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install curl_cffi for yt-dlp TikTok impersonation (not on PyPI with yt-dlp extras)
RUN pip install --no-cache-dir "curl_cffi>=0.7"

# Upgrade yt-dlp to latest nightly for best TikTok support
RUN pip install --no-cache-dir --upgrade "yt-dlp[default]"

COPY app.py .

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
