# Site2Local
> **Mildly Powerful and IT'S FREE!!** · **V9.0**

A **Vibe-coded** python project which makes a existent website running on localhost or any external ip address!

> **WARNING: This tool is being constantly updated and it is still in "W.I.P", expect errors, missing things and garbage.**

> **This is V9.0 — the modularized version of site2local.** The monolith `s2l.py` (frozen at 8.1) is kept at the repo root as the reference for the match-based modularization (see `MAPA_MODULAR.md`). The version lives in `Config.VERSION`.

# Documentary
It's accessible in [here!](https://github.com/RealThalesGit/Site2Local/blob/main/DOCUMENTARY.md)

## Repository layout — ONE folder

The repo root **is** the package: everything lives inside a single
`Site2Local/` folder — code, the monolith reference, launcher, docs.

```
Site2Local/
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
├── MAPA_MODULAR.md     ← symbol-level map: modular tree ↔ s2l.py line numbers
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

python3 Site2Local/Boot.py          # ← the simple way (from the parent folder)
cd Site2Local && python3 Boot.py    # same thing, from inside
python -m Site2Local                # equivalent (from the parent folder)
python Site2Local/s2l.py            # the monolith (reference / comparison)
```

`Boot.py` (and `python -m Site2Local`) `cd`s into the `Site2Local` folder
before starting, so the mirror (`site_src/`), metadata (`site_data/`) and
wordlists always land **inside that same folder**, no matter where you
invoke it from.

Optional packages (each unlocks a feature — see the comments inside
`requirements.txt`): `curl-cffi` (best Cloudflare bypass), `ai-cloudscraper`
(fallback bypass), `brotli`, `wafw00f`, `tqdm` (SCAN_PATHS progress bar) and
`PyQt6` (HOOK_GUI traffic inspector).

Before starting, edit `Site2Local/Config.py`: `SITE` is the target, plus
flags like `CRAWL`, `OFFLINE`, `PROXY_CDN`, `HOOK_GUI`, `RAINBOW_LOGS`.

## What i can change on it?
You can change if you want or don't want to use the "dump all" (only avaliable in 7.0-8.0), you can enable or disable crawling (if you have already crawled the site, or you want crawling in demand only) 

Change headers to mobile, tablet, desktop, macintosh, ie11, iphone, ipad, symbian, and bot. 
When you change DEVICE and restart, cached pages are re-fetched from the site once automatically, so you never get stuck serving the layout of the previous profile (site assets stay cached — only HTML documents refresh).

Optional thing is that also you can enable rainbow logs (It will use lolcat if your system has it, but make sure to install lolcat before!), and there is so much others you can do.

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
