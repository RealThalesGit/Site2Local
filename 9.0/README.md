# Site2Local
> **Mildly Powerful and IT'S FREE!!** · **V9.0**

A **Vibe-coded** python project which makes a existent website running on localhost or any external ip address!

> **WARNING: This tool is being constantly updated and it is still in "W.I.P", expect errors, missing things and garbage.**

> **This is V9.0 — the modularized version of site2local.** The monolith `s2l.py` (frozen at 8.1) is kept at the repo root as the reference for the match-based modularization (see `MODULAR_MAP.md`). The version lives in `Config.VERSION`.

# Documentary
It's accessible in [here!](https://github.com/RealThalesGit/Site2Local/blob/main/DOCUMENTARY.md)

## What's new in 9.0?

- **Modularized!** The 13.7k-line single file became ~100 small modules under one folder, migrated symbol-by-symbol with a zero-diff match. The monolith `s2l.py` stays frozen at 8.1 as the reference, and `MODULAR_MAP.md` documents every symbol → line mapping plus the full parity audit (config 27/27, routes 10/10, smoke 9/9, 0 symbols lost).
- **Device-profile cache revalidation**: change `DEVICE` between boots and every cached HTML page is re-fetched once with the new profile — no more phone layout stuck on your desktop mirror.
- **Coherent client hints**: explicit device profiles now send the matching `Sec-CH-UA*` headers, so hint-first backends can't override the profile you picked.
- **Firefox/Safari fix**: cross-origin isolation (COOP/COEP) is now browser-aware — no more `NS_ERROR_DOM_CORP_FAILED` on images when you open a game in Firefox.
- **CSS custom properties with `url(...)` are rewritten too** (`setProperty` patch in the injector), so icons defined like `--icon-src: url(https://cdn/x.svg)` load from your local CDN port instead of the real one.
- **Content-based MIME rescue**: scripts behind exotic extensions (like `globe.gl`, which the stdlib types as `video/gl`) are sniffed and served as `text/javascript`, with self-healing ETags that un-poison browsers which already cached the wrong type.
- **Extensionless cache MIME rescue**: files cached without a path extension (query-only URLs like Google Fonts' `css2?family=Montserrat`) are content-sniffed too — a cached stylesheet now serves as `text/css` instead of `application/octet-stream`, which browsers with `nosniff` used to reject with `NS_ERROR_CORRUPTED_CONTENT` on every reload (it worked on the first visit, then never again). JSON APIs cached at extensionless paths stopped the purge-and-refetch loop.

## 9.0 Directory Structure

```
9.0/
├── Boot.py             ← run with python3 Site2Local/Boot.py (simple launcher)
├── s2l.py              ← the original monolith (13.7k lines) — still runs standalone
├── Config.py           ← user settings (edit before starting)
├── Runtime.py          ← shared state + startup orchestration (the hub)
├── __init__.py         ← the ONE __init__.py of the whole tree
├── Core/               ← URL, cache naming, MIME, compression, stats, logging
├── HTTP/               ← headers, upstream sessions, streaming/SSE
├── Proxy/              ← the main proxy pipeline (Routes/), cache, upstream
├── Crawler/            ← bulk crawler (Crawl/) and hidden-path scanner (Scanner/)
├── CDN/                ← multiport CDN host registry + servers
├── WebSockets/         ← RFC 6455 protocol, tunnels, upstream transports
├── Rewriting/          ← HTML/JSON URL rewriting + the s2l injector
├── Capture/            ← HTTP + WebSocket capture-to-disk workers
├── Hooks/              ← hook engine + PyQt6 GUI (HOOK_GUI=True)
├── Viewers/            ← web viewers (CDN dashboard, stats, health)
├── Network/            ← WSGI server + TCP/UDP tunnels
├── CLI/                ← the startup banner
├── MODULAR_MAP.md      ← symbol-level map: modular tree ↔ s2l.py line numbers
├── requirements.txt
└── .gitignore
```

Every subfolder is a **namespace package** (PEP 420): the whole tree has
exactly **one** `__init__.py` (the package root). `Runtime.py` imports leaf
modules directly, and the historical `import s2l` still works when running
from inside the folder.

## Install & run

```bash
pip install -r Site2Local/requirements.txt   # core only (flask, requests, bs4, colorama)

python3 9.0/Boot.py          # ← the simple way (from the parent folder)
cd 9.0 && python3 Boot.py    # same thing, from inside
python -m Site2Local                # equivalent (from the parent folder)
python 9.0/s2l.py            # the monolith (reference / comparison)
```

`Boot.py` (and `python -m Site2Local`) `cd`s into the `9.0` folder
before starting, so the mirror (`site_src/`), metadata (`site_data/`) and
wordlists always land **inside that same folder**, no matter where you
invoke it from.

Optional packages (each unlocks a feature — see the comments inside
`requirements.txt`): `curl-cffi` (best Cloudflare bypass), `ai-cloudscraper`
(fallback bypass), `brotli`, `wafw00f`, `tqdm` (SCAN_PATHS progress bar) and
`PyQt6` (HOOK_GUI traffic inspector).

Before starting, edit `9.0/Config.py`: `SITE` is the target, plus
flags like `CRAWL`, `OFFLINE`, `PROXY_CDN`, `HOOK_GUI`, `RAINBOW_LOGS`.

## What i can change on it?
You can change if you want or don't want to use the "dump all" (`DUMP_ALL` — it extracts and crawls every URL found in any response body), you can enable or disable crawling (if you have already crawled the site, or you want crawling in demand only), and you can go full `OFFLINE` (serve the disk mirror only, zero upstream requests).

Change headers to mobile, tablet, desktop, macintosh, ie11, iphone, ipad, symbian, and bot (or leave `auto` to mirror the UA of whoever is visiting your mirror). 
When you change DEVICE and restart, cached pages are re-fetched from the site once automatically, so you never get stuck serving the layout of the previous profile (site assets stay cached — only HTML documents refresh).

Optional thing is that also you can enable rainbow logs (It will use lolcat if your system has it, but make sure to install lolcat before!), and there is so much others you can do — the hidden-path scanner (`SCAN_PATHS`), traffic capture (`CAPTURE` / `CAPTURE_WS`), the PyQt6 hook GUI (`HOOK_GUI`), `SHOW_HIDDEN` to un-hide display:none elements...

## What i shouldn't do with it?
This tool is not and was never made to create malicious and phishing websites, so don't make me the villain of the history, use it only for yourself, but if you want to host a website that use this tool, please, just try to not make the website literally identical to the original one if you are going to host something that use this tool, and here's the warning again, dont use it for malicious purposes, the traffic hook system/gui only affects your side (client-side), it does not affect the "server-side" part of the website, if it doesn't have an security failure;

## Can i edit the downloaded target website files?
Yes, you can edit free like a bird, editing the website you are only affects your side (aka client-side). The mirror lands in `site_src/<site>/` (git-ignored, it's yours).

## Is this project based of another project?
Nope, site2local is independent and constantly being updated, i don't push the updates REALLY often though.

## Known bugs
If you cache too many websites on your pc, crawl may start failing to get some files, it is recommended to always clean your browser cache, i recommend using ["BleachBit"](https://www.bleachbit.org/download).
If some page fail to start (eg, game content isn't downloading) refresh the page

## Optional Things
Don't forget to share, fork it and contribute to it!

## Things to know
Since i am the only one upgrading it, when it isn't updated in a while, it is because i am busy with other stuff or i just ran out of ideas for a moment, i am sorry if some updates take days, weeks or months.

License: Apache 2.0
