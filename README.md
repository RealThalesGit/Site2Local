# Site2Local
> **Mildly Powerful and IT'S FREE!!**

A **Vibe-coded** python project which makes a existent website running on localhost or any external ip address!

> **WARNING: This tool is being constantly updated and it is still in "W.I.P", expect errors, missing things and garbage.**

> **The current version is 9.0, which is the new generation of site2local, the single-file layout has been modularized.**

## 9.0 — the "new generation" line
- **The legacy migration bridges are GONE**: caches written by older
  layouts (pre-9.1 `_= encodings, the 9.1a value-only names, the 09/10
  host-tree files) are no longer renamed on request — such a URL is a
  plain MISS (online re-fetches into the current name, offline 404s).
- **Startup garbage sweep**: unreachable
  legacy caches and orphan `.q` sidecars are deleted at startup, with a
  per-file log + a reclaimed-space summary. Unconditional and provably
  safe — a file goes only when NO candidate URL maps back to it via
  `local_path()`; live pretty files, collision fallbacks and capped
  names are always kept.
- **Query strings in the access log**: every HIT/304/206/EXT/STREAM line
  now shows `path?query`, so `__query__` variants are distinguishable
  (`GET HIT host/asset/app.js?v=aaa111 818.0B`).
- **Dead-code cleanup**: migration machinery removed (`Legacy.py`,
  `Migration.py`, `_migration_checked` state, frozen 9.1a formula, the
  09/10 lossy sanitizer); the startup banner now prints the real
  `Config.VERSION` (was a stale hardcoded "V 8 . 1").

## Still true from the 9.x line
- **Beautified `__query__` names**: every query parameter joins its NAME
  with its value — `Aya.App.js?v=26b38…` caches as `Aya.App_v_26b38….js`,
  `/v1/asset?id=7` as `asset_id_7` (2+ params keep the
  `[k1]_[v1]_N_[k2]_[v2]` shape, `N` = "&"). A tiny `.q` sidecar records
  the owning query so collisions fall back safely, and overlong joined
  names are capped so the cache write never fails on filesystem limits.
- **WebAssembly isolation hardened**: pages that need `crossOriginIsolated`
  (Emscripten pthread apps — e.g. aya.nodium.lol) get COOP+COEP on
  documents, **iframes** and **workers/worklets**, including HTML served
  live through CDN ports and the `/__s2l_ext__/` route. Chromium and
  Firefox ≥119 negotiate COEP `credentialless` so anonymous third-party
  assets keep loading.
- **Content-based MIME resolver (unified)**: cache files whose name lies
  about their type are rescued by sniffing — magic bytes
  (WASM/PNG/JPEG/WEBP/woff2/mp4), JSON, JavaScript, CSS. Cured:
  Google-Fonts `css2` served as `octet-stream` (broke a Next.js
  marketplace) and `globe.gl` served as `video/gl` (broke a 3D site's
  globe — black blob).
- **ETag MIME auto-heal** + real `206` responses for Range requests on
  CDN/ext cache hits.

See `MODULAR_MAP.md` for the full module map and response pipeline.

# Documentary
It's accessible in [here!](https://github.com/RealThalesGit/Site2Local/blob/main/DOCUMENTARY.md)

## What i can change on it?
You can change if you want or don't want to use the "dump all" (DUMP_ALL), you can enable or disable crawling (if you have already crawled the site, or you want crawling in demand only) 

Change headers to mobile, tablet, desktop, macintosh, ie11, iphone, ipad, symbian, and bot. 

Optional thing is that also you can enable rainbow logs (It will use lolcat if your system has it, but make sure to install lolcat before!), and there is so much others you can do.

## What i shouldn't do with it?
This tool is not and was never made to create malicious and phishing websites, so don't make me the villain of the history, use it only for yourself, but if you want to host a website that use this tool, please, just try to not make the website literally identical to the original one if you are going to host something that use this tool, and here's the warning again, dont use it for malicious purposes, the traffic hook system/gui only affects your side (client-side), it does not affect the "server-side" part of the website, if it doesn't have an security failure;

## Can i edit the downloaded target website files?
Yes, you can edit free like a bird, editing the website you are only affects your side (aka client-side)

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
