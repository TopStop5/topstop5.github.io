import os
import re
import io
import html
import json
import time
import random
import asyncio
import tempfile
import textwrap
import zipfile
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify, send_file, Response, stream_with_context
from flask_cors import CORS
from ebooklib import epub

app = Flask(__name__)
CORS(app)
@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


# ── Site Config Registry ───────────────────────────────────────────────────────
# To add a new site, add one entry here. No other code changes needed.
#
# Keys:
#   content_sel   : CSS selector for the chapter text container
#   title_sel     : CSS selector for the chapter title (optional)
#   url_pattern   : chapter URL template — use {base} and {num}
#   remove_sels   : list of CSS selectors to strip before extracting text
#   stop_phrases  : list of strings — stop collecting paragraphs when seen
#   sentinel      : string in page body that means "chapter doesn't exist"
#   needs_js      : True = fall back to Selenium for this site

SITE_CONFIG = {
    "novelfire.net": {
        "content_sel":  "#content",
        "title_sel":    "span.chapter-title",
        "url_pattern":  "{base}/chapter-{num}",
        "remove_sels":  [".nf-ads"],
        "stop_phrases": [
            "If you find any errors",
            "Share to your friends",
            "Tap the middle of the screen",
        ],
        "sentinel":     "Some novel pages moved for better user experience",
        "needs_js":     False,
    },
    "wetriedtls.com": {
        "content_sel":  "#reader-container",
        "title_sel":    "h1.entry-title",
        "url_pattern":  "{base}/chapter-{num}",
        "remove_sels":  [],
        "stop_phrases": [],
        "sentinel":     None,
        "needs_js":     False,
    },
    "webnoveltranslations.net": {
        "content_sel":  "#novel-chapter-container",
        "title_sel":    "h1",
        "url_pattern":  "{base}/chapter-{num}/",
        "remove_sels":  [],
        "stop_phrases": [],
        "sentinel":     None,
        "needs_js":     False,
    },
}

MAX_CONCURRENT = 3
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 2.0
REQUEST_DELAY  = 0.5


# ── Custom Exceptions ──────────────────────────────────────────────────────────

class ChapterNotFound(Exception):
    """Chapter does not exist on the site."""

class UnsupportedSite(Exception):
    pass


# ── URL Helpers ────────────────────────────────────────────────────────────────

def get_domain(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")


def get_base_url(url: str) -> str:
    """Strip any trailing /chapter-N so we have the clean novel base URL."""
    return re.sub(r"/chapter-\d+/?$", "", url.rstrip("/"))


def get_novel_title_from_url(url: str) -> str:
    """Best-effort title from the URL slug."""
    parts = urlparse(url).path.strip("/").split("/")
    for part in reversed(parts):
        if part and not part.startswith("chapter"):
            return part.replace("-", " ").title()
    return "Novel"


# ── Text Extraction ────────────────────────────────────────────────────────────

def extract_chapter_text(html_text: str, config: dict, ch_num: int) -> str:
    """Parse HTML and return clean chapter text, double-spaced between paragraphs."""
    soup = BeautifulSoup(html_text, "html.parser")

    # Sentinel check — means chapter doesn't exist
    if config.get("sentinel") and config["sentinel"] in soup.get_text():
        raise ChapterNotFound(f"Chapter {ch_num} does not exist")

    # Find content container
    container = soup.select_one(config["content_sel"])
    if not container:
        raise Exception(
            f"Content container '{config['content_sel']}' not found for chapter {ch_num}"
        )

    # Remove ad / junk nodes
    for sel in config.get("remove_sels", []):
        for el in container.select(sel):
            el.decompose()

    # Extract title
    title_text = ""
    if config.get("title_sel"):
        title_el = soup.select_one(config["title_sel"])
        if title_el:
            title_text = title_el.get_text(strip=True)

    # Extract paragraphs
    stop_phrases = config.get("stop_phrases", [])
    lines = []
    if title_text:
        lines.append(title_text)

    paragraphs = container.find_all("p")
    if paragraphs:
        for p in paragraphs:
            text = p.get_text(strip=True)
            if not text or text == title_text:
                continue
            if any(text.startswith(phrase) for phrase in stop_phrases):
                break
            lines.append(text)
    else:
        # Fallback: raw text split
        for line in container.get_text(separator="\n").split("\n"):
            line = line.strip()
            if not line or line == title_text:
                continue
            if any(line.startswith(phrase) for phrase in stop_phrases):
                break
            lines.append(line)

    if not lines:
        raise Exception(f"No text extracted for chapter {ch_num}")

    # Double-space: every paragraph separated by a blank line (\n\n)
    return "\n\n".join(lines)


# ── Async Fetcher ──────────────────────────────────────────────────────────────

def fetch_chapter_sync(chapter_url: str, config: dict, ch_num: int) -> str:
    """
    Sync fetch using curl_cffi to impersonate a real Chrome TLS fingerprint,
    bypassing Cloudflare Turnstile / Bot Management (the 'Just a moment...' page).
    """
    last_exc = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            time.sleep(REQUEST_DELAY)
            r = cffi_requests.get(
                chapter_url,
                impersonate="chrome120",
                timeout=20,
            )
            print(f"CH{ch_num} status={r.status_code} url={r.url}")
            print(f"CH{ch_num} body preview: {r.text[:300]}")

            if r.status_code == 404:
                raise ChapterNotFound(f"Chapter {ch_num} returned 404")
            r.raise_for_status()
            return extract_chapter_text(r.text, config, ch_num)

        except ChapterNotFound:
            raise  # never retry a missing chapter

        except Exception as e:
            last_exc = e
            if attempt < RETRY_ATTEMPTS - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))

    raise last_exc


async def fetch_chapter(
    loop,
    sem: asyncio.Semaphore,
    chapter_url: str,
    config: dict,
    ch_num: int,
) -> str:
    """Async wrapper — runs curl_cffi in a thread executor so it doesn't block the event loop."""
    async with sem:
        return await loop.run_in_executor(
            None, fetch_chapter_sync, chapter_url, config, ch_num
        )


async def scrape_all_chapters(
    base_url: str,
    start: int,
    end: int,
    config: dict,
    progress_cb=None,
) -> tuple:
    """
    Fetch chapters concurrently.
    progress_cb(done, total, ch_num, ok, end_of_novel=None) called per chapter.
    Returns (chapters_dict, failed_list).
    """
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    chapters = {}
    failed = []
    total = end - start + 1
    url_pattern = config["url_pattern"]
    loop = asyncio.get_event_loop()

    # Create all tasks upfront so they can run concurrently
    tasks = {
        ch_num: asyncio.create_task(
            fetch_chapter(
                loop, sem,
                url_pattern.format(base=base_url, num=ch_num),
                config, ch_num
            )
        )
        for ch_num in range(start, end + 1)
    }

    done_count = 0
    novel_ended = False

    for ch_num in range(start, end + 1):
        if novel_ended:
            tasks[ch_num].cancel()
            continue

        try:
            text = await tasks[ch_num]
            chapters[ch_num] = text
            ok = True
        except ChapterNotFound:
            novel_ended = True
            ok = False
            done_count += 1
            if progress_cb:
                progress_cb(done_count, total, ch_num, ok, end_of_novel=ch_num)
            continue
        except Exception:
            failed.append(ch_num)
            ok = False

        done_count += 1
        if progress_cb:
            progress_cb(done_count, total, ch_num, ok)

    return chapters, failed


# ── Selenium Fallback ──────────────────────────────────────────────────────────

def scrape_with_selenium(
    base_url: str, start: int, end: int, config: dict, progress_cb=None
):
    """Only used when config['needs_js'] is True."""
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    opts = Options()
    for arg in [
        "--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
        "--disable-gpu", "--disable-extensions",
        "--blink-settings=imagesEnabled=false", "--log-level=3",
        "--window-size=1280,800",
    ]:
        opts.add_argument(arg)
    opts.binary_location = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
    service = Service(
        executable_path=os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver"),
        log_path=os.devnull,
    )
    driver = webdriver.Chrome(service=service, options=opts)

    chapters, failed = {}, []
    total = end - start + 1
    url_pattern = config["url_pattern"]

    try:
        for i, ch_num in enumerate(range(start, end + 1), 1):
            chapter_url = url_pattern.format(base=base_url, num=ch_num)
            try:
                driver.get(chapter_url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        ("css selector", config["content_sel"])
                    )
                )
                text = extract_chapter_text(driver.page_source, config, ch_num)
                chapters[ch_num] = text
                ok = True
            except ChapterNotFound:
                if progress_cb:
                    progress_cb(i, total, ch_num, False, end_of_novel=ch_num)
                break
            except Exception:
                failed.append(ch_num)
                ok = False

            if progress_cb:
                progress_cb(i, total, ch_num, ok)
            time.sleep(random.uniform(0.4, 0.9))
    finally:
        driver.quit()

    return chapters, failed


# ── Output Builders ────────────────────────────────────────────────────────────

def build_epub(chapters_dict: dict, novel_title: str) -> bytes:
    def safe(name):
        return re.sub(r'[\\/*?:"<>|]', "-", name).strip()

    book = epub.EpubBook()
    book.set_identifier(safe(novel_title.lower()) or "novel")
    book.set_title(novel_title)
    book.set_language("en")
    book.add_author("TopStop5's NovelScraper")

    epub_chapters = []
    zero_pad = len(str(max(chapters_dict.keys())))

    for num in sorted(chapters_dict.keys()):
        text = chapters_dict[num]
        chap_title = f"Chapter {num}"
        # Each paragraph is already separated by \n\n — split and render as <p> tags
        paras = [p.strip() for p in re.split(r"\n\n", text) if p.strip()]
        html_body = "".join(f"<p>{html.escape(p)}</p>" for p in paras)
        html_doc = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<html xmlns="http://www.w3.org/1999/xhtml">'
            f"<head><title>{html.escape(chap_title)}</title></head>"
            f"<body>{html_body}</body></html>"
        )
        ch = epub.EpubHtml(
            title=chap_title,
            file_name=f"chapter_{str(num).zfill(zero_pad)}.xhtml",
            lang="en",
        )
        ch.content = html_doc.encode("utf-8")
        book.add_item(ch)
        epub_chapters.append(ch)

    book.toc = epub_chapters
    book.spine = ["nav"] + epub_chapters
    book.add_item(epub.EpubNcx())
    book.add_item(epub.EpubNav())

    with tempfile.NamedTemporaryFile(suffix=".epub", delete=False) as f:
        tmp_path = f.name
    epub.write_epub(tmp_path, book)
    with open(tmp_path, "rb") as f:
        data = f.read()
    os.unlink(tmp_path)
    return data


def build_zip(chapters_dict: dict) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for num in sorted(chapters_dict.keys()):
            zf.writestr(f"Chapter {num}.txt", chapters_dict[num])
    buf.seek(0)
    return buf.read()


# ── SSE Helper ─────────────────────────────────────────────────────────────────

def sse(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status": "ok",
        "message": "TopStop5 Novel Scraper API",
        "routes": ["/", "/health", "/scrape", "/scrape-stream"],
        "supported_sites": list(SITE_CONFIG.keys()),
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


def _validate(data):
    """Shared validation. Returns (url, fmt, start, end, error_response)."""
    url = str(data.get("url", "")).strip()
    fmt = str(data.get("format", "txt")).strip().lower()
    try:
        start = int(data.get("start", 1))
        end   = int(data.get("end", 1))
    except (TypeError, ValueError):
        return None, None, None, None, (jsonify({"error": "start/end must be integers"}), 400)

    if not url:
        return None, None, None, None, (jsonify({"error": "No URL provided."}), 400)
    if fmt not in {"txt", "epub"}:
        return None, None, None, None, (jsonify({"error": "Invalid format."}), 400)
    if start < 1 or end < 1:
        return None, None, None, None, (jsonify({"error": "Chapters must be >= 1."}), 400)
    if end < start:
        return None, None, None, None, (jsonify({"error": "End must be >= start."}), 400)
    if end - start > 99:
        return None, None, None, None, (jsonify({"error": "Max 100 chapters per request."}), 400)
    return url, fmt, start, end, None


@app.route("/scrape-stream", methods=["POST"])
def scrape_stream():
    """
    SSE streaming endpoint.
    Sends per-chapter progress events, then on completion delivers
    the file base64-encoded in the final 'done' event so the browser
    can trigger a download without a second request.
    """
    data = request.get_json(silent=True) or {}
    url, fmt, start, end, err = _validate(data)
    if err:
        return err

    domain = get_domain(url)
    config = SITE_CONFIG.get(domain)
    if not config:
        return jsonify({"error": f"Unsupported site: {domain}"}), 400

    base_url    = get_base_url(url)
    novel_title = get_novel_title_from_url(url)

    def generate():
        import threading, base64

        events       = []
        scrape_result = {}
        scrape_error  = {}

        def progress_cb(done, total, ch_num, ok, end_of_novel=None):
            pct = int((done / total) * 90)
            ev = {"type": "progress", "done": done, "total": total,
                  "ch": ch_num, "ok": ok, "pct": pct}
            if end_of_novel:
                ev["end_of_novel"] = end_of_novel
            events.append(ev)

        def run_scrape():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                if config.get("needs_js"):
                    chaps, fails = scrape_with_selenium(
                        base_url, start, end, config, progress_cb
                    )
                else:
                    chaps, fails = loop.run_until_complete(
                        scrape_all_chapters(base_url, start, end, config, progress_cb)
                    )
                scrape_result["chapters"] = chaps
                scrape_result["failed"]   = fails
            except Exception as e:
                scrape_error["msg"] = str(e)
            finally:
                loop.close()

        thread = threading.Thread(target=run_scrape, daemon=True)
        thread.start()

        # Forward events to client as they arrive
        last_sent = 0
        while thread.is_alive() or last_sent < len(events):
            while last_sent < len(events):
                yield sse(events[last_sent])
                last_sent += 1
            time.sleep(0.1)

        # Drain any final events
        while last_sent < len(events):
            yield sse(events[last_sent])
            last_sent += 1

        if scrape_error:
            yield sse({"type": "error", "message": scrape_error["msg"]})
            return

        chapters = scrape_result.get("chapters", {})
        failed   = scrape_result.get("failed", [])

        if not chapters:
            yield sse({"type": "error", "message": "No chapters could be scraped."})
            return

        yield sse({"type": "progress", "pct": 93, "message": "Building file..."})

        safe_title = re.sub(r'[\\/*?:"<>|]', "-", novel_title).strip() or "Novel"

        try:
            if fmt == "epub":
                file_bytes = build_epub(chapters, novel_title)
                filename   = f"{safe_title}.epub"
                mimetype   = "application/epub+zip"
            else:
                file_bytes = build_zip(chapters)
                filename   = f"{safe_title}.zip"
                mimetype   = "application/zip"
        except Exception as e:
            yield sse({"type": "error", "message": f"File build failed: {e}"})
            return

        yield sse({
            "type":     "done",
            "pct":      100,
            "filename": filename,
            "mimetype": mimetype,
            "chapters": len(chapters),
            "failed":   failed,
            "data":     base64.b64encode(file_bytes).decode("utf-8"),
        })

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",  # disable nginx buffering on Railway
        },
    )


@app.route("/scrape", methods=["POST"])
def scrape():
    """Legacy non-streaming endpoint. Kept for backwards compatibility."""
    data = request.get_json(silent=True) or {}
    url, fmt, start, end, err = _validate(data)
    if err:
        return err

    domain = get_domain(url)
    config = SITE_CONFIG.get(domain)
    if not config:
        return jsonify({"error": f"Unsupported site: {domain}"}), 400

    base_url    = get_base_url(url)
    novel_title = get_novel_title_from_url(url)

    try:
        if config.get("needs_js"):
            chapters, failed = scrape_with_selenium(base_url, start, end, config)
        else:
            loop = asyncio.new_event_loop()
            chapters, failed = loop.run_until_complete(
                scrape_all_chapters(base_url, start, end, config)
            )
            loop.close()
    except Exception as e:
        return jsonify({"error": f"Scrape failed: {e}"}), 500

    if not chapters:
        return jsonify({"error": "No chapters scraped.", "failed": failed}), 500

    safe_title = re.sub(r'[\\/*?:"<>|]', "-", novel_title).strip() or "Novel"

    if fmt == "epub":
        buf = io.BytesIO(build_epub(chapters, novel_title))
        return send_file(buf, mimetype="application/epub+zip",
                         as_attachment=True, download_name=f"{safe_title}.epub")

    buf = io.BytesIO(build_zip(chapters))
    return send_file(buf, mimetype="application/zip",
                     as_attachment=True, download_name=f"{safe_title}.zip")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)