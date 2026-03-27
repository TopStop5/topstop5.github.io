# TopStop5

A personal corner of the internet. Dark, fast, a little obsessive about purple. Tools I built because I wanted them to exist.

🔗 **[topstop5.github.io](https://topstop5.github.io)**

-----

## What’s here

**Novel Scraper** — I wanted to download novels as epubs from the web so i built this. This tool lets you pull web novel content straight to your browser. Configure your source, set your chapter range, and let it run.

**Checker** — A utility for checking minecraft usernames. 

**API** — A Python backend for the Novel Scraper. Idk what else to say.
-----

## The vibe

Black background. Purple glows. Monospace labels. A grid that hums quietly behind everything.

Fonts are [Rajdhani](https://fonts.google.com/specimen/Rajdhani) for headings and [Share Tech Mono](https://fonts.google.com/specimen/Share+Tech+Mono) for the technical bits. No frameworks, no build step — just HTML, CSS, and JavaScript doing exactly what they’re supposed to do.

-----

## Running the API

```bash
cd api
docker build -t topstop5-api .
docker run -p 8000:8000 topstop5-api
```

The frontend deploys itself — push to `main`, GitHub Pages handles the rest.

-----

## Structure

```
├── index.html          # home
├── novelscraper.html   # chapter scraper
├── checker.html        # username checker
└── api/                # python backend for novelscraper + dockerfile
```