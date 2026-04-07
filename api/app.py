import os
import re
import io
import html
import json
import time
import random
import asyncio
import tempfile
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
SITE_CONFIG = {
    "novelfire.net": {
        "content_sel":  "#content",
        "title_sel":    "span.chapter-title",
        "cover_sel":    "figure.cover img",
        "url_pattern":  "{base}/chapter-{num}",
        "remove_sels":  [".nf-ads"],
        "stop_phrases": [
            "If you find any errors",
            "Share to your friends",
            "Tap the middle of the screen",
        ],
        "sentinel":     "Some novel pages moved for better user experience",
        "needs_js":     False,
        "impersonate":  "chrome124",   # chrome124 now 403s; try latest fingerprint
        "extra_headers": {            # extra headers help pass CF bot checks
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
        },
    },
    "wetriedtls.com": {
        "content_sel":  "#reader-container",
        "title_sel":    None,
        "cover_sel":    "img.rounded",
        "url_pattern":  "{base}/chapter-{num}",
        "remove_sels":  [],
        "stop_phrases": [],
        "sentinel":     None,
        "needs_js":     True,   # Next.js/React - Selenium waits for #reader-container to render
        "hr_separator": True,   # split TL credit from chapter at <div data-type="horizontalRule">
    },
    "webnoveltranslations.net": {
        "content_sel":  "#novel-chapter-container",
        "title_sel":    "h1",
        "cover_sel":    ".novel-cover img",
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
REQUEST_DELAY  = time.sleep(random.uniform(1.5, 3.5))
DEFAULT_IMPERSONATE = "chrome124"


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


# ── Cover Fetcher ──────────────────────────────────────────────────────────────

def fetch_cover_image(base_url: str, config: dict):
    """
    Fetch the novel cover image bytes + media type from the novel index page.
    Returns (image_bytes, media_type) or (None, None) if unavailable.
    """
    cover_sel = config.get("cover_sel")
    if not cover_sel:
        return None, None

    impersonate = config.get("impersonate", DEFAULT_IMPERSONATE)

    try:
        r = cffi_requests.get(base_url, impersonate=impersonate, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        img = soup.select_one(cover_sel)
        if not img or not (img.get("src") or img.get("srcset")):
            print(f"Cover selector '{cover_sel}' not found at {base_url}")
            return None, None

        # WeTriedTLS uses Next.js /_next/image?url=<encoded_real_url> — decode it
        img_url = img.get("src", "")
        if "/_next/image" in img_url and "url=" in img_url:
            from urllib.parse import parse_qs, urlparse as _up, unquote
            qs = parse_qs(_up(img_url).query)
            img_url = unquote(qs.get("url", [""])[0])
        if not img_url:
            return None, None

        if img_url.startswith("//"):
            img_url = "https:" + img_url
        elif img_url.startswith("/"):
            parsed = urlparse(base_url)
            img_url = f"{parsed.scheme}://{parsed.netloc}{img_url}"

        print(f"Fetching cover: {img_url}")
        img_r = cffi_requests.get(img_url, impersonate=impersonate, timeout=20)
        img_r.raise_for_status()
        ext = img_url.split("?")[0].rsplit(".", 1)[-1].lower()
        media_type = {
            "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "png": "image/png", "webp": "image/webp", "gif": "image/gif",
        }.get(ext, "image/jpeg")
        return img_r.content, media_type

    except Exception as e:
        print(f"Cover fetch failed: {e}")
        return None, None


# ── Text Extraction ────────────────────────────────────────────────────────────

def _find_key_recursive(obj, key):
    """Depth-first search for a key in a nested dict/list structure."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = _find_key_recursive(v, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _find_key_recursive(item, key)
            if result is not None:
                return result
    return None


def extract_chapter_text(html_text: str, config: dict, ch_num: int) -> str:
    """Parse HTML and return clean chapter text, double-spaced between paragraphs."""
    soup = BeautifulSoup(html_text, "html.parser")

    # Sentinel check
    if config.get("sentinel") and config["sentinel"] in soup.get_text():
        raise ChapterNotFound(f"Chapter {ch_num} does not exist")

    # ── Next.js __NEXT_DATA__ extraction ──────────────────────────────────────
    # Sites like WeTriedTLS render content via React client-side. The raw HTTP
    # response is a JS shell, but the chapter HTML is serialised inside
    # <script id="__NEXT_DATA__">. We extract it directly — no Selenium needed.
    next_data_key   = config.get("next_data_content_key")
    next_data_probe = config.get("next_data_probe", False)

    # Common keys used by Next.js novel sites to store chapter HTML/text
    _PROBE_KEYS = [
        "content", "chapterContent", "chapter_content", "body", "text",
        "chapterBody", "novelContent", "html", "chapterHtml", "chapterText",
        "data", "chapter", "chapterData",
    ]

    def _all_keys(obj, prefix=""):
        keys = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                full = f"{prefix}.{k}" if prefix else k
                keys.append(full)
                keys.extend(_all_keys(v, full))
        elif isinstance(obj, list) and obj:
            keys.extend(_all_keys(obj[0], f"{prefix}[]"))
        return keys

    if next_data_key or next_data_probe:
        next_script = soup.find("script", id="__NEXT_DATA__")
        if next_script:
            try:
                page_data  = json.loads(next_script.string)
                page_props = page_data.get("props", {}).get("pageProps", {})

                raw = None
                found_key = None

                # Try the configured key first
                if next_data_key:
                    raw = _find_key_recursive(page_props, next_data_key)
                    if raw and isinstance(raw, str) and len(raw) > 50:
                        found_key = next_data_key
                    else:
                        raw = None

                # Auto-probe if no configured key matched
                if raw is None and next_data_probe:
                    for probe_key in _PROBE_KEYS:
                        candidate = _find_key_recursive(page_props, probe_key)
                        if candidate and isinstance(candidate, str) and len(candidate) > 50:
                            raw = candidate
                            found_key = probe_key
                            print(f"CH{ch_num}: __NEXT_DATA__ auto-probe found key='{probe_key}' "
                                  f"({len(raw)} chars) — set next_data_content_key='{probe_key}' "
                                  f"in SITE_CONFIG to skip probing next time")
                            break
                    if raw is None:
                        all_k = _all_keys(page_props)
                        print(f"CH{ch_num}: __NEXT_DATA__ probe failed. "
                              f"All pageProps keys: {all_k[:40]}")

                if raw and found_key:
                    soup = BeautifulSoup(f'<div id="nd-root">{raw}</div>', "html.parser")
                    print(f"CH{ch_num}: __NEXT_DATA__ OK key='{found_key}' ({len(raw)} chars)")

            except Exception as e:
                print(f"CH{ch_num}: __NEXT_DATA__ parse error: {e}")
        else:
            print(f"CH{ch_num}: no __NEXT_DATA__ script tag found")

    # Find content container
    container = soup.select_one(config["content_sel"])
    # For __NEXT_DATA__ sites the original content_sel won't exist in the
    # re-parsed soup — fall back to the injected root div
    if not container and (next_data_key or next_data_probe):
        container = soup.select_one("#nd-root")
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

    stop_phrases = config.get("stop_phrases", [])

    # ── WeTriedTLS special handling ────────────────────────────────────────────
    # The site wraps the TL's intro/credit block before a <div data-type="horizontalRule">,
    # and the actual chapter text comes after it. We skip everything before the first HR.
    if config.get("hr_separator"):
        from bs4 import Tag, NavigableString
        hr_divs = container.find_all("div", attrs={"data-type": "horizontalRule"})
        if hr_divs:
            HR_MARKER = "-" * 60
            chapter_els = []
            recording = False
            for el in container.children:
                if el == hr_divs[0]:
                    recording = True
                    continue
                if recording:
                    chapter_els.append(el)

            lines = []
            if title_text:
                lines.append(title_text)
            for el in chapter_els:
                if isinstance(el, NavigableString):
                    t = str(el).strip()
                    if t:
                        lines.append(t)
                    continue
                if not isinstance(el, Tag):
                    continue
                if el.get("data-type") == "horizontalRule":
                    lines.append(HR_MARKER)
                    continue
                if el.name == "p":
                    text = el.get_text(strip=True)
                    if text and text != title_text:
                        if any(text.startswith(ph) for ph in stop_phrases):
                            break
                        lines.append(text)
                else:
                    for p in el.find_all("p"):
                        text = p.get_text(strip=True)
                        if text and text != title_text:
                            if any(text.startswith(ph) for ph in stop_phrases):
                                break
                            lines.append(text)

            if not lines:
                raise Exception(f"No text extracted for chapter {ch_num}")
            return "\n\n".join(lines)

        # No HR found — fall through to generic extraction below
        print(f"WeTriedTLS CH{ch_num}: no horizontalRule div found, using generic extraction")

    # ── Generic extraction ─────────────────────────────────────────────────────
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
        for line in container.get_text(separator="\n").split("\n"):
            line = line.strip()
            if not line or line == title_text:
                continue
            if any(line.startswith(phrase) for phrase in stop_phrases):
                break
            lines.append(line)

    if not lines:
        raise Exception(f"No text extracted for chapter {ch_num}")

    return "\n\n".join(lines)


# ── Sync Fetcher ───────────────────────────────────────────────────────────────

def fetch_chapter_sync(chapter_url: str, config: dict, ch_num: int) -> str:
    """
    Sync fetch using curl_cffi to impersonate a real Chrome TLS fingerprint,
    bypassing Cloudflare Turnstile / Bot Management.
    """
    impersonate  = config.get("impersonate", DEFAULT_IMPERSONATE)
    extra_headers = config.get("extra_headers", {})
    last_exc = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            time.sleep(REQUEST_DELAY)
            r = cffi_requests.get(
                chapter_url,
                impersonate=impersonate,
                headers=extra_headers or None,
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
        except Exception as e:
            import traceback
            print(f"CH{ch_num} FETCH ERROR: {e}")
            traceback.print_exc()
            failed.append(ch_num)
            ok = False

        done_count += 1
        if progress_cb:
            progress_cb(done_count, total, ch_num, ok)

        # ── Retry failed chapters (up to 2 more attempts) ─────────────────────────
    if failed:
        for retry_round in range(1, 3):
            if not failed:
                break
            print(f"Retry round {retry_round} for chapters: {failed}")
            still_failed = []
            retry_tasks = {
                ch_num: asyncio.create_task(
                    fetch_chapter(
                        loop, sem,
                        url_pattern.format(base=base_url, num=ch_num),
                        config, ch_num
                    )
                )
                for ch_num in failed
            }
            for ch_num in failed:
                try:
                    text = await retry_tasks[ch_num]
                    chapters[ch_num] = text
                    if progress_cb:
                        progress_cb(done_count, total, ch_num, True)
                except ChapterNotFound:
                    still_failed.append(ch_num)
                except Exception as e:
                    print(f"CH{ch_num} RETRY {retry_round} ERROR: {e}")
                    still_failed.append(ch_num)
            failed = still_failed

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
            except Exception as e:
                import traceback
                print(f"CH{ch_num} SELENIUM ERROR: {e}")
                traceback.print_exc()
                failed.append(ch_num)
                ok = False

            if progress_cb:
                progress_cb(i, total, ch_num, ok)
            time.sleep(random.uniform(0.4, 0.9))
    finally:
        driver.quit()

    return chapters, failed


# ── Output Builders ────────────────────────────────────────────────────────────

def build_epub(
    chapters_dict: dict,
    novel_title: str,
    cover_image: bytes = None,
    cover_media_type: str = "image/jpeg",
) -> bytes:
    def safe(name):
        return re.sub(r'[\\/*?:"<>|]', "-", name).strip()

    book = epub.EpubBook()
    book.set_identifier(safe(novel_title.lower()) or "novel")
    book.set_title(novel_title)
    book.set_language("en")
    book.add_author("TopStop5's Novel Scraper")

    if cover_image:
        ext = cover_media_type.split("/")[-1].replace("jpeg", "jpg")
        # set_cover() internally adds the image item — don't call add_item()
        # separately or the zip will contain a duplicate entry and warn/corrupt.
        book.set_cover(f"images/cover.{ext}", cover_image)

    epub_chapters = []
    zero_pad = len(str(max(chapters_dict.keys())))

    for num in sorted(chapters_dict.keys()):
        text = chapters_dict[num]
        chap_title = f"Chapter {num}"
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
    if end - start > 9999:
        return None, None, None, None, (jsonify({"error": "Max 10,000 chapters per request."}), 400)
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

        events        = []
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

        # Emit start event so the frontend can display the novel title
        yield sse({"type": "start", "title": novel_title})

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

        cover_image, cover_media_type = None, "image/jpeg"
        if fmt == "epub":
            cover_image, cover_media_type = fetch_cover_image(base_url, config)

        try:
            if fmt == "epub":
                file_bytes = build_epub(
                    chapters, novel_title,
                    cover_image, cover_media_type or "image/jpeg",
                )
                filename = f"{safe_title}.epub"
                mimetype = "application/epub+zip"
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
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/parse", methods=["POST"])
def parse():
    """
    Accepts raw HTML + site domain + chapter number from the browser.
    Used by client_fetch sites where Railway cannot reach the target host.
    Returns {"text": "...", "ok": true} or {"error": "...", "ok": false}.
    """
    data   = request.get_json(silent=True) or {}
    domain = str(data.get("domain", "")).strip()
    ch_num = int(data.get("ch", 0))
    html_text = str(data.get("html", ""))

    config = SITE_CONFIG.get(domain)
    if not config:
        return jsonify({"ok": False, "error": f"Unsupported site: {domain}"}), 400
    if not html_text:
        return jsonify({"ok": False, "error": "No HTML provided"}), 400

    try:
        text = extract_chapter_text(html_text, config, ch_num)
        return jsonify({"ok": True, "text": text})
    except ChapterNotFound as e:
        return jsonify({"ok": False, "error": str(e), "end_of_novel": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/scrape-client-stream", methods=["POST"])
def scrape_client_stream():
    """
    SSE endpoint for client_fetch sites.
    The browser fetches each chapter itself and POSTs batches of
    {ch, html} objects here. We parse and stream progress back.
    Body: {url, format, start, end, chapters: [{ch, html}, ...]}
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
    raw_chapters = data.get("chapters", [])  # [{ch: N, html: "..."}]

    def generate():
        import base64
        chapters = {}
        failed   = []
        total    = end - start + 1

        yield sse({"type": "start", "title": novel_title})

        for i, item in enumerate(raw_chapters, 1):
            ch_num   = int(item.get("ch", 0))
            html_text = item.get("html", "")
            try:
                text = extract_chapter_text(html_text, config, ch_num)
                chapters[ch_num] = text
                ok = True
            except ChapterNotFound as e:
                ok = False
                yield sse({"type": "progress", "done": i, "total": total,
                           "ch": ch_num, "ok": False, "pct": int((i/total)*90),
                           "end_of_novel": ch_num})
                break
            except Exception as e:
                failed.append(ch_num)
                ok = False

            yield sse({"type": "progress", "done": i, "total": total,
                       "ch": ch_num, "ok": ok, "pct": int((i/total)*90)})

        if not chapters:
            yield sse({"type": "error", "message": "No chapters could be parsed."})
            return

        yield sse({"type": "progress", "pct": 93, "message": "Building file..."})

        safe_title = re.sub(r'[\\/*?:"<>|]', "-", novel_title).strip() or "Novel"
        cover_image, cover_media_type = None, "image/jpeg"
        if fmt == "epub":
            cover_image, cover_media_type = fetch_cover_image(base_url, config)

        try:
            if fmt == "epub":
                file_bytes = build_epub(chapters, novel_title, cover_image,
                                        cover_media_type or "image/jpeg")
                filename = f"{safe_title}.epub"
                mimetype = "application/epub+zip"
            else:
                file_bytes = build_zip(chapters)
                filename   = f"{safe_title}.zip"
                mimetype   = "application/zip"
        except Exception as e:
            yield sse({"type": "error", "message": f"File build failed: {e}"})
            return

        yield sse({
            "type": "done", "pct": 100,
            "filename": filename, "mimetype": mimetype,
            "chapters": len(chapters), "failed": failed,
            "data": base64.b64encode(file_bytes).decode("utf-8"),
        })

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
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
        cover_image, cover_media_type = fetch_cover_image(base_url, config)
        buf = io.BytesIO(build_epub(
            chapters, novel_title,
            cover_image, cover_media_type or "image/jpeg",
        ))
        return send_file(buf, mimetype="application/epub+zip",
                         as_attachment=True, download_name=f"{safe_title}.epub")

    buf = io.BytesIO(build_zip(chapters))
    return send_file(buf, mimetype="application/zip",
                     as_attachment=True, download_name=f"{safe_title}.zip")


# Run smoke test on import (catches gunicorn worker startup failures early)
import threading as _st


def selenium_smoke_test():
    """Run once at startup to verify Chromium/ChromeDriver are working."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options
        opts = Options()
        for arg in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
                    "--disable-gpu", "--disable-extensions", "--log-level=3"]:
            opts.add_argument(arg)
        opts.binary_location = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
        svc = Service(
            executable_path=os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver"),
            log_path=os.devnull,
        )
        driver = webdriver.Chrome(service=svc, options=opts)
        driver.get("about:blank")
        driver.quit()
        print("Selenium smoke test PASSED")
    except Exception as e:
        import traceback
        print(f"Selenium smoke test FAILED: {e}")
        traceback.print_exc()


_st.Thread(target=selenium_smoke_test, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, threaded=True)