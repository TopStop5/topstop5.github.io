import os
import re
import time
import random
import textwrap
import html
import tempfile
import zipfile
import io
from urllib.parse import urlparse

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

try:
    from ebooklib import epub
except ImportError:
    os.system("pip install EbookLib")
    from ebooklib import epub

app = Flask(__name__)
CORS(app)


# ── Chrome driver ──────────────────────────────────────────────────────────────

def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--log-level=3")

    chrome_bin = os.environ.get("CHROME_BIN", "/usr/bin/google-chrome")
    chromedriver_path = os.environ.get("CHROMEDRIVER_PATH", "/usr/bin/chromedriver")

    opts.binary_location = chrome_bin
    service = Service(executable_path=chromedriver_path, log_path=os.devnull)
    return webdriver.Chrome(service=service, options=opts)


# ── Helpers ────────────────────────────────────────────────────────────────────

def save_as_epub(chapters_dict, novel_title):
    """chapters_dict: {chapter_num: content_str}. Returns epub bytes."""

    def safe(name):
        return re.sub(r'[\\/*?:"<>|]', '-', name).strip()

    book = epub.EpubBook()
    book.set_identifier(safe(novel_title.lower()) or "novel")
    book.set_title(novel_title)
    book.set_language("en")
    book.add_author("TopStop5's Novelscraper")

    epub_chapters = []
    zero_pad = len(str(max(chapters_dict.keys())))

    for num in sorted(chapters_dict.keys()):
        text = chapters_dict[num]
        chap_title = f"Chapter {num}"
        paras = [p.strip() for p in re.split(r'(?:\r?\n){2,}', text) if p.strip()]
        html_body = ''.join(f"<p>{html.escape(p)}</p>" for p in paras)
        html_doc = (
            f'<?xml version="1.0" encoding="utf-8"?>'
            f'<html xmlns="http://www.w3.org/1999/xhtml">'
            f"<head><title>{html.escape(chap_title)}</title></head>"
            f"<body>{html_body}</body></html>"
        )

        ch = epub.EpubHtml(
            title=chap_title,
            file_name=f"chapter_{str(num).zfill(zero_pad)}.xhtml",
            lang="en"
        )
        ch.content = html_doc
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


def make_zip(chapters_dict):
    """Returns zip bytes of all chapters as .txt files."""
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for num in sorted(chapters_dict.keys()):
            zf.writestr(f"Chapter {num}.txt", chapters_dict[num])

    buf.seek(0)
    return buf.read()


# ── NovelFire ──────────────────────────────────────────────────────────────────

def normalize_novelfire_url(url):
    parsed = urlparse(url)
    parts = parsed.path.strip("/").split("/")

    if "book" in parts:
        idx = parts.index("book")
        base = "/".join(parts[:idx + 2])
        return f"{parsed.scheme}://{parsed.netloc}/{base}"

    return url.rstrip("/")


def scrape_novelfire_chapter(driver, base_url, ch_num):
    chapter_url = f"{base_url}/chapter-{ch_num}"

    try:
        driver.get(chapter_url)
    except TimeoutException:
        driver.execute_script("window.stop();")

    time.sleep(1.5)

    container = driver.find_element(By.ID, "chapter-container")

    title_text = ""
    try:
        title_text = driver.find_element(By.CLASS_NAME, "chapter-title").text.strip()
    except Exception:
        pass

    try:
        content_root = container.find_element(By.CLASS_NAME, "chapter-content")
    except Exception:
        content_root = container

    for ad in content_root.find_elements(By.CSS_SELECTOR, ".nf-ads"):
        driver.execute_script("arguments[0].remove();", ad)

    final_lines = []
    if title_text:
        final_lines.append(title_text)

    paragraphs = content_root.find_elements(By.XPATH, ".//p")
    for p in paragraphs:
        text = p.text.strip()
        if not text:
            continue
        if (
            text.startswith("If you find any errors")
            or text.startswith("Share to your friends")
            or "Tap the middle of the screen" in text
        ):
            break
        if text == title_text:
            continue
        final_lines.append(text)

    if len(final_lines) <= (1 if title_text else 0):
        for line in content_root.text.split("\n"):
            line = line.strip()
            if not line or line == title_text:
                continue
            if line.startswith("If you find any errors"):
                break
            final_lines.append(line)

    wrapped = [textwrap.fill(line, width=80) for line in final_lines]
    return "\n\n".join(wrapped)


def scrape_novelfire(novel_url, start, end):
    base_url = normalize_novelfire_url(novel_url)
    driver = make_driver()
    chapters = {}
    failed = []

    try:
        driver.get(novel_url)
        time.sleep(2)

        try:
            parsed = urlparse(novel_url)
            parts = parsed.path.strip("/").split("/")
            idx = parts.index("book")
            slug = parts[idx + 1]
            novel_title = slug.replace("-", " ").title()
        except Exception:
            novel_title = "Novel"

        for ch in range(start, end + 1):
            try:
                content = scrape_novelfire_chapter(driver, base_url, ch)
                chapters[ch] = content
            except Exception:
                failed.append(ch)

        for ch in list(failed):
            try:
                time.sleep(2)
                content = scrape_novelfire_chapter(driver, base_url, ch)
                chapters[ch] = content
                failed.remove(ch)
            except Exception:
                pass

    finally:
        driver.quit()

    return novel_title, chapters, failed


# ── WeTriedTLS ─────────────────────────────────────────────────────────────────

def scrape_wetriedtls(novel_url, start, end):
    driver = make_driver()
    chapters = {}
    failed = []
    novel_title = "Novel"

    try:
        driver.get(novel_url)
        time.sleep(2)

        try:
            novel_title = driver.find_element(By.TAG_NAME, "h1").text.strip()
        except Exception:
            novel_title = driver.title.split("|")[0].strip()

        for ch_num in range(start, end + 1):
            chapter_url = f"{novel_url.rstrip('/')}/chapter-{ch_num}"
            try:
                driver.get(chapter_url)
                WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located((By.ID, "reader-container"))
                )
                container = driver.find_element(By.ID, "reader-container")
                paragraphs = container.find_elements(By.TAG_NAME, "p")
                lines = [p.text.strip() for p in paragraphs if p.text.strip()]
                chapters[ch_num] = "\n\n".join(lines)
            except Exception:
                failed.append(ch_num)

            time.sleep(random.uniform(0.5, 1.2))

        for ch in list(failed):
            try:
                chapter_url = f"{novel_url.rstrip('/')}/chapter-{ch}"
                driver.get(chapter_url)
                WebDriverWait(driver, 15).until(
                    EC.visibility_of_element_located((By.ID, "reader-container"))
                )
                container = driver.find_element(By.ID, "reader-container")
                paragraphs = container.find_elements(By.TAG_NAME, "p")
                lines = [p.text.strip() for p in paragraphs if p.text.strip()]
                chapters[ch] = "\n\n".join(lines)
                failed.remove(ch)
            except Exception:
                pass

    finally:
        driver.quit()

    return novel_title, chapters, failed


# ── WebnovelTranslations ───────────────────────────────────────────────────────

def scrape_webnoveltranslations(novel_url, start, end):
    driver = make_driver()
    chapters = {}
    failed = []
    novel_title = "Novel"

    try:
        driver.get(novel_url)
        time.sleep(2)

        try:
            novel_title = driver.find_element(By.TAG_NAME, "h1").text.strip()
        except Exception:
            novel_title = driver.title.split("|")[0].strip()

        novel_title = re.sub(
            r"(\s*[-:]\s*Chapter\s*\d+)$",
            "",
            novel_title,
            flags=re.IGNORECASE
        )

        for ch_num in range(start, end + 1):
            base = novel_url.rstrip("/")
            chapter_url = f"{base}/chapter-{ch_num}/"

            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])

            try:
                driver.get(chapter_url)
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div#novel-chapter-container"))
                )
                container = driver.find_element(By.CSS_SELECTOR, "div#novel-chapter-container")
                paragraphs = container.find_elements(By.TAG_NAME, "p")
                lines = [p.text.strip() for p in paragraphs if p.text.strip()]

                if lines:
                    chapters[ch_num] = "\n\n".join(lines)
                else:
                    failed.append(ch_num)

            except Exception:
                failed.append(ch_num)

            finally:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(random.uniform(0.5, 1.2))

    finally:
        driver.quit()

    return novel_title, chapters, failed


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/", methods=["GET"])
def root():
    return jsonify({
        "status": "ok",
        "message": "TopStop5 Novel Scraper API is running",
        "routes": ["/", "/health", "/scrape"]
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/scrape", methods=["POST"])
def scrape():
    data = request.get_json(silent=True) or {}

    url = str(data.get("url", "")).strip()
    fmt = str(data.get("format", "txt")).strip().lower()

    try:
        start = int(data.get("start", 1))
        end = int(data.get("end", 1))
    except (TypeError, ValueError):
        return jsonify({"error": "Start and end must be valid integers."}), 400

    if start < 1 or end < 1:
        return jsonify({"error": "Start and end must be 1 or greater."}), 400

    if end < start:
        return jsonify({"error": "End chapter must be greater than or equal to start chapter."}), 400

    if end - start > 99:
        return jsonify({"error": "Maximum 100 chapters per request."}), 400

    if not url:
        return jsonify({"error": "No URL provided."}), 400

    if fmt not in {"txt", "epub"}:
        return jsonify({"error": "Invalid format. Use 'txt' or 'epub'."}), 400

    if "novelfire" in url:
        scrape_fn = scrape_novelfire
    elif "wetriedtls" in url:
        scrape_fn = scrape_wetriedtls
    elif "webnoveltranslations" in url:
        scrape_fn = scrape_webnoveltranslations
    else:
        return jsonify({
            "error": "Unsupported site. Supported: novelfire, wetriedtls, webnoveltranslations"
        }), 400

    try:
        novel_title, chapters, failed = scrape_fn(url, start, end)
    except Exception as e:
        return jsonify({"error": f"Scrape failed: {str(e)}"}), 500

    if not chapters:
        return jsonify({"error": "No chapters could be scraped.", "failed": failed}), 500

    safe_title = re.sub(r'[\\/*?:"<>|]', "-", novel_title).strip() or "Novel"

    if fmt == "epub":
        epub_bytes = save_as_epub(chapters, novel_title)
        buf = io.BytesIO(epub_bytes)
        buf.seek(0)
        return send_file(
            buf,
            mimetype="application/epub+zip",
            as_attachment=True,
            download_name=f"{safe_title}.epub"
        )

    zip_bytes = make_zip(chapters)
    buf = io.BytesIO(zip_bytes)
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{safe_title}.zip"
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)