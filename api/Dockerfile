FROM python:3.11-slim

# curl_cffi bundles its own libcurl-impersonate — no system deps needed.
# Only add these if you ever enable needs_js: True for a site (Selenium).
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
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8080

# gthread workers: each request gets its own thread, so asyncio.run() per request
# is fully isolated. 4 workers x 4 threads = 16 concurrent scrapes without event loop conflicts.
CMD gunicorn app:app --bind 0.0.0.0:$PORT --timeout 300 --workers 4 --worker-class gthread --threads 4