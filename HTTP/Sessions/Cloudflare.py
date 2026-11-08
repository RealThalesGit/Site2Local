"""Site2Local: upstream HTTP session management."""
from __future__ import annotations


from ... import Runtime as _s2l_state


def _next_cf_config(mobile: bool) -> dict:
    """Round-robin through browser configs to avoid CF fingerprint blacklisting."""

    with _s2l_state._cf_config_lock:
        if mobile:
            return {"browser": "chrome", "platform": "android", "desktop": False}
        cfg = _s2l_state._CF_BROWSER_CONFIGS[_s2l_state._cf_config_idx % len(_s2l_state._CF_BROWSER_CONFIGS)]
        _s2l_state._cf_config_idx += 1
        return cfg


def _is_cf_block(body: bytes, status: int, headers: dict | None = None) -> bool:
    """Detect Cloudflare challenge/block pages.

    Covers three distinct signal classes:
      • Classic 403/503 HTML block pages (Ray ID, cf-browser-verification)
      • CF Managed Challenge / Turnstile — these return HTTP 200, not 403/503,
        with a JS challenge embedded in the body (the "q=78" / jschl_vc family
        reported against some CDNs). A status-only check misses these entirely.
      • The cf-mitigated: challenge response header, when present — the most
        reliable single signal Cloudflare gives us, independent of body content.
    """
    if headers:
        h = {k.lower(): v.lower() for k, v in headers.items()}
        if h.get("cf-mitigated") == "challenge":
            return True

    if not body:
        return False

    head = body[:8192].lower()

    # Classic block — these always carry one of these markers on 403/503
    if status in (403, 503):
        if (b"cf-browser-verification" in head
                or b"please enable cookies" in head
                or b"checking your browser" in head
                or b"cf-error-overview" in head
                or (b"ray id" in head and b"cloudflare" in head)
                or (b"attention required" in head and b"cloudflare" in head)):
            return True

    # JS challenge / Managed Challenge — can return 200, so checked regardless
    # of status. Signature set includes the q=78 / jschl_vc challenge tokens.
    if (b"jschl_vc" in head
            or b"jschl_answer" in head
            or b"cf-challenge" in head
            or b"cf_chl_prog" in head
            or b"__cf_chl_tk__" in head
            or b"__cf_chl_f_tk" in head
            or b"chl_captcha_widget" in head
            or b"window._cf_chl_opt" in head
            or (b"challenges.cloudflare.com" in head and b"<script" in head)):
        return True

    # Modern Turnstile + "Just a moment" interstitials.
    # These return 200 with a JS challenge body that doesn't always carry
    # the classic jschl_vc tokens — the CF team rotates signatures frequently.
    if status in (200, 403, 503):
        if (b"just a moment" in head
                or b"cf-turnstile" in head
                or b"cf_turnstile" in head
                or b"turnstile.min.js" in head
                or b"challenges.cloudflare.com/turnstile" in head
                or (b"checking if the site connection is secure" in head)
                or (b"enable javascript and cookies" in head)
                or (b"cf-please-wait" in head)):
            return True

    # Generic WAF blocks — "access denied", "blocked",
    # "request blocked" returned as HTML with a 403/503. These were slipping
    # through because _is_raw_block_text only catches SHORT non-HTML bodies.
    if status in (403, 503) and b"<html" in head:
        if (b"access denied" in head
                or b"request blocked" in head
                or b"blocked by" in head
                or b"security check" in head
                or b"verify you are human" in head):
            return True

    return False


def _is_raw_block_text(body: bytes, status: int) -> bool:
    # Never treat 429 as a WAF block — it's a legitimate rate-limit signal.
    # Treating it as a block causes a retry that doubles the request rate,
    # pushing the API into a sustained 429 loop.
    if status not in (200, 403, 503):
        return False
    stripped = body.strip().lower()
    if not stripped or len(stripped) > 256:
        return False
    return any(p in stripped for p in _s2l_state._RAW_BLOCK_TEXT)


def _is_bot_page(body: bytes, status: int = 200, path: str = "") -> bool:
    """Return True if the body is a bot-detection / CAPTCHA page.

    Only fires on HTML responses — JSON API 403s are never bot pages.
    Path-exempt prefixes (internal API routes, /api/, etc.) are skipped.
    """
    # API endpoints return legitimate 4xx — never block them
    if path and any(path.startswith(p) for p in _s2l_state._BOT_EXEMPT_PREFIXES):
        return False
    if _s2l_state._is_raw_block_text(body, status):
        return True
    if len(body) < 64:
        return False
    # Must look like HTML
    head = body[:16384].lower()
    if b"<html" not in head and b"<!doctype" not in head:
        return False
    if _s2l_state._is_cf_block(body, status, None):
        return True
    for sig in _s2l_state._BOT_PAGE_SIGNATURES:
        if sig in head:
            return True
    return False
