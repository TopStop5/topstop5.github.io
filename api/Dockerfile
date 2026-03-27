FROM python:3.11-slim

# Chrome/Selenium is no longer needed for the default config (all sites use httpx).
# If you ever add a site with needs_js: True, uncomment the block below.
#
# RUN apt-get update && apt-get install -y \
#     chromium chromium-driver fonts-liberation libnss3 \
#     libatk-bridge2.0-0 libx11-xcb1 libxcb-dri3-0 libxcomposite1 \
#     libxrandr2 libgbm1 libgtk-3-0 libasound2 \
#  && rm -rf /var/lib/apt/lists/*
# ENV CHROME_BIN=/usr/bin/chromium
# ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

ENV PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080

# 2 workers × 4 threads handles concurrent SSE streams.
# Timeout 120s is plenty — no Selenium cold-start overhead.
CMD gunicorn app:app --bind 0.0.0.0:$PORT --timeout 120 --workers 1 --worker-class gevent