"""Site2Local: HTML visibility, service-worker and URL rewriting."""
from __future__ import annotations

from urllib.parse import urlparse
import re

from ... import Runtime as _s2l_state


def _rewrite_ext_urls(html_bytes: bytes, html_doc: bool = False) -> bytes:
    """Rewrite ALL external https:// URLs in HTML attributes, srcset, and CSS url().

    html_doc=True marks HTML *documents* (vs standalone .css files): inline
    style="..." attributes are then rewritten by a quote-preserving pass (see
    below) instead of the generic url() rewriter, whose double-quote output
    is only safe inside a stylesheet file.
    """
    if not _s2l_state.PROXY_CDN or not html_bytes:
        return html_bytes
    try:
        html = html_bytes.decode("utf-8", "ignore")
    except Exception:
        return html_bytes

    def _attr_rep(m):
        host, tail = m.group(3), m.group(4) or "/"
        if _s2l_state._is_dont_rewrite_host(host):
            return m.group(0)
        r = _s2l_state._proxy_target(host, tail)
        return f"{m.group(1)}{m.group(2)}{r}{m.group(5)}" if r else m.group(0)
    # Path is optional (/? instead of /) so bare-host URLs like
    # href="https://example.com" (no trailing slash) also get rewritten.
    html = re.sub(
        r'((?<![A-Za-z0-9_$.])(?:src|href|poster|data-src|data-href|action)\s*=\s*)(["\'])https?://([a-zA-Z0-9\-._]+(?::\d+)?)(/[^"\'<>]*)?(["\'])',
        _attr_rep, html, flags=re.IGNORECASE)

    # Protocol-relative attribute values (src="//host/path") resolve against the
    # CURRENT page origin — for any host other than the document's own, the
    # browser connects to the REAL site directly (e.g. a consent/loader
    # script shipped as src="//site.example/assets/loader.js") — a proxy leak. Rewrite
    # them like their https:// siblings. The lookbehind keeps JS property
    # assignments (j.src = '//api...') out of scope — those belong to the
    # inline-script pass.
    def _attr_rep_pr(m):
        host, tail = m.group(3), m.group(4) or "/"
        if _s2l_state._is_dont_rewrite_host(host):
            return m.group(0)
        r = _s2l_state._proxy_target(host, tail)
        return f"{m.group(1)}{m.group(2)}{r}{m.group(5)}" if r else m.group(0)
    html = re.sub(
        r'((?<![A-Za-z0-9_$.])(?:src|href|poster|data-src|data-href|action)\s*=\s*)(["\'])//([a-zA-Z0-9\-._]+(?::\d+)?)(/[^"\'<>]*)?(["\'])',
        _attr_rep_pr, html, flags=re.IGNORECASE)

    def _ss_rep(m):
        parts = []
        # Split only on a comma followed by whitespace — the real srcset candidate
        # separator. Image-CDN URLs (Cloudflare /cdn-cgi/image/q=78,w=300,.../...)
        # embed un-spaced commas directly in the path; splitting on every comma
        # truncates those URLs at the first option.
        for e in re.split(r',\s+', m.group(2).strip()):
            e = e.strip()
            if not e: continue
            bits = e.split(None, 1)
            url = bits[0]; desc = (" " + bits[1]) if len(bits) > 1 else ""
            # Handle https://, http://, and protocol-relative //host/path
            if url.startswith(("https://", "http://")):
                p = urlparse(url)
                if _s2l_state._is_dont_rewrite_host(p.netloc):
                    parts.append(url + desc)
                    continue
                r = _s2l_state._proxy_target(p.netloc, p.path + (f"?{p.query}" if p.query else ""))
                if r: url = r
            elif url.startswith("//"):
                p = urlparse("https:" + url)
                if _s2l_state._is_dont_rewrite_host(p.netloc):
                    parts.append(url + desc)
                    continue
                r = _s2l_state._proxy_target(p.netloc, p.path + (f"?{p.query}" if p.query else ""))
                if r: url = r
            parts.append(url + desc)
        return m.group(1) + ", ".join(parts) + m.group(3)
    html = re.sub(r'(srcset\s*=\s*["\'])([^"\']+)(["\'])', _ss_rep, html, flags=re.IGNORECASE)

    # CSS url() rewriting — TWO quote-aware passes:
    #
    # 1. QUOTED urls: url("...") / url('...'). The string body may contain
    #    parentheses (font/image paths like Background%20(3)%20(1).png occur
    #    frequently in site-builder templates), so the match must end at the CLOSING QUOTE,
    #    not at the first ')'. The old single regex stopped at the '(' inside
    #    such paths, glued the tail ('.png");...') onto the rewritten URL and
    #    produced an UNBALANCED url( token — the browser's CSS parser then
    #    swallowed every rule after it up to the next '}'. On a large real
    #    site that ate ~1 800 rules including the whole navbar block,
    #    collapsing the menu into an unstyled vertical list with black text
    #    and wrapped caret icons.
    # 2. UNQUOTED urls: tolerate ONE level of balanced parentheses, then trim
    #    a trailing ')' that isn't part of the URL.
    # Rewritten values are always re-quoted with double quotes — valid CSS in
    # every context and immune to parens/commas in the proxied path.
    #
    # ⚠ EXCEPT inside inline style="..." attributes: there the value lives
    # INSIDE an HTML attribute delimited by " or ', so re-quoting with "
    # CLOSES the attribute mid-URL. Real-world pattern (SVG icons exposed as
    # a CSS custom property, common in icon systems):
    #   style="--icon-src: url('https://cdn.example.com/icons/ui/icon.svg');"
    # became
    #   style="--icon-src: url("http://.../icon.svg");"
    # The parser ended the attribute at the inserted quote and turned the rest
    # of the URL into garbage attributes (http:, localhost:8081, icons, ui,
    # icon.svg");") — the custom property lost its value and every --icon-src
    # icon vanished. HTML documents therefore extract style attributes first,
    # rewrite their url() tokens PRESERVING the original quote character, and
    # hide the results behind placeholders until every generic pass is done.
    def _css_url_from(u: str) -> str | None:
        p = urlparse(u)
        if not p.netloc or _s2l_state._is_dont_rewrite_host(p.netloc):
            return None
        tail = p.path + (f"?{p.query}" if p.query else "")
        if not tail:
            return None
        return _s2l_state._proxy_target(p.netloc, tail)

    _style_bodies: list[str] = []

    def _rw_style_body(body: str, delim: str) -> str:
        # 1. Quoted url('...') / url("...") — keep the ORIGINAL quote char.
        def _q(m: re.Match) -> str:
            r = _css_url_from(m.group(2))
            return f'url({m.group(1)}{r}{m.group(1)})' if r else m.group(0)
        body = re.sub(
            r'url\(\s*([\'"])(https?://[a-zA-Z0-9\-._]+(?::\d+)?/[^\'"]*)\1\s*\)',
            _q, body, flags=re.IGNORECASE)
        # 2. Entity-quoted url(&quot;...) / url(&#39;...) — SSR frameworks
        #    escape quotes inside style attributes instead of mixing them;
        #    rewrite back into the SAME entity form.
        def _ent(m: re.Match) -> str:
            r = _css_url_from(m.group(2))
            return f'url({m.group(1)}{r}{m.group(1)})' if r else m.group(0)
        body = re.sub(
            r'url\(\s*(&(?:quot|#39|#x27);?)((?:[^&]|&(?!quot;|#39;|#x27;))*?)\1\s*\)',
            _ent, body, flags=re.IGNORECASE)
        # 3. Unquoted url(https://...) — stay unquoted while the rewritten URL
        #    is attribute-safe; once it contains parens/quotes/whitespace,
        #    quote it with the character that is NOT the attribute delimiter
        #    (always valid inside the attribute and valid CSS).
        _STYLE_UNSAFE = re.compile(r'[()"\'\s]')
        def _u(m: re.Match) -> str:
            u = m.group(1)
            while u and u.count("(") < u.count(")"):
                u = u[:-1]
            if not u:
                return m.group(0)
            r = _css_url_from(u)
            if not r:
                return m.group(0)
            if not _STYLE_UNSAFE.search(r):
                return f"url({r})"
            alt = "'" if delim == '"' else '"'
            return f"url({alt}{r}{alt})"
        body = re.sub(
            r'url\(\s*(https?://[a-zA-Z0-9\-._]+(?::\d+)?/?(?:[^()\s]|\([^()\s]*\))*)\s*\)',
            _u, body, flags=re.IGNORECASE)
        return body

    def _style_attr_rep(m: re.Match) -> str:
        pre, delim, body = m.group(1), m.group(2), m.group(3)
        _style_bodies.append(_rw_style_body(body, delim))
        return f"{pre}{delim}\x00S2LSTYLE{len(_style_bodies) - 1}\x00{delim}"

    if html_doc:
        # (?<!...[-]) keeps data-style / *-style look-alikes out of scope.
        html = re.sub(
            r'((?<![A-Za-z0-9_.$-])style\s*=\s*)(["\'])(.*?)\2',
            _style_attr_rep, html, flags=re.IGNORECASE | re.DOTALL)

    def _css_quoted_rep(m):
        r = _css_url_from(m.group(2))
        return f'url("{r}")' if r else m.group(0)

    def _css_unquoted_rep(m):
        u = m.group(1)
        while u and u.count("(") < u.count(")"):
            u = u[:-1]
        if not u:
            return m.group(0)
        r = _css_url_from(u)
        return f'url("{r}")' if r else m.group(0)

    html = re.sub(
        r'url\(\s*(["\'])(https?://[a-zA-Z0-9\-._]+(?::\d+)?/[^"\']*)\1\s*\)',
        _css_quoted_rep, html, flags=re.IGNORECASE)
    html = re.sub(
        r'url\(\s*(https?://[a-zA-Z0-9\-._]+(?::\d+)?/?(?:[^()\s]|\([^()\s]*\))*)\s*\)',
        _css_unquoted_rep, html, flags=re.IGNORECASE)

    # Strip CSP delivered via <meta http-equiv="Content-Security-Policy" ...> —
    # the HTTP header is already stripped in filter_resp, but pages that set CSP
    # via meta tag (frame-ancestors, script-src, etc.) bypass that and still get
    # enforced by the browser, blocking framing/scripts loaded through the proxy.
    html = re.sub(
        r'<meta[^>]+http-equiv\s*=\s*["\']content-security-policy["\'][^>]*>',
        '', html, flags=re.IGNORECASE)

    # Strip <base href="..."> tags — a base tag pointing at the real CDN host
    # changes how ALL relative URLs in the document resolve, causing every
    # relative src/href to bypass the proxy and go straight to the real internet.
    # The proxy already rewrites all absolute URLs, so the base tag is redundant.
    html = re.sub(r'<base\s[^>]*>', '', html, flags=re.IGNORECASE)

    # Strip <link rel="preconnect"> and <link rel="dns-prefetch"> tags.
    # These hint the browser to open a TCP/TLS connection to the real CDN host
    # BEFORE the rewritten URL is fetched — but the rewritten URL points to
    # localhost, not the real CDN. The preconnect to the real host is wasted
    # work AND can trigger CORS/TLS errors when the browser sees the real host
    # trying to set cookies it can't validate against localhost. Removing them
    # is safe — the proxy is the only connection the browser ever needs.
    html = re.sub(
        r'<link\b[^>]*\brel\s*=\s*["\'](?:preconnect|dns-prefetch)["\'][^>]*>',
        '', html, flags=re.IGNORECASE)

    # ── Strip SRI (integrity="...") attributes from <script> and <link> tags ──
    # CRITICAL FIX: s2l intentionally rewrites URLs inside CSS/JS resources so
    # they load through the proxy. This changes the bytes, which changes the
    # hash, which makes the browser REFUSE to apply the resource — the page
    # then renders with NO styles and NO scripts (the classic "blank-ish page"
    # symptom on sites that ship SRI on their CSS bundle).
    # Stripping integrity= here is the equivalent of stripping CSP: we are
    # intentionally modifying the content, so the integrity check is wrong.
    # The crossorigin attribute is left alone — s2l already serves everything
    # with Access-Control-Allow-Origin: *, so anonymous CORS works fine.
    html = re.sub(
        r'\s+integrity\s*=\s*["\'][^"\']*["\']',
        '', html, flags=re.IGNORECASE)

    # <meta http-equiv="refresh" content="0;url=https://host/path"> — the URL
    # here is never the first thing after the quote (it's "N;url=..."), so the
    # attribute regex above never matches it. A meta-refresh is a real, raw
    # navigation the browser performs on its own, same risk as an unrewritten
    # location.href: it goes straight at the real host and can be refused.
    def _meta_refresh_rep(m):
        tag = m.group(0)
        if not re.search(r'http-equiv\s*=\s*["\']refresh["\']', tag, re.IGNORECASE):
            return tag
        def _content_rep(cm):
            host, tail = cm.group(2), cm.group(3) or ""
            r = _s2l_state._proxy_target(host, tail)
            return f"{cm.group(1)}{r}{cm.group(4)}" if r else cm.group(0)
        return re.sub(
            r'(content\s*=\s*["\'][^"\']*?url\s*=\s*)https?://([a-zA-Z0-9\-._]+(?::\d+)?)(/[^"\'<>]*)?(["\'])',
            _content_rep, tag, flags=re.IGNORECASE)
    html = re.sub(r'<meta\b[^>]*>', _meta_refresh_rep, html, flags=re.IGNORECASE)

    # ── Rewrite CDN URLs inside <script> blocks ───────────────────────────────
    # Hydration payloads (framework state embedded as JSON inside a <script>
    # tag — Next.js __NEXT_DATA__, Redux/Nuxt/Remix state, etc.) commonly embed
    # absolute CDN URLs that client-side JS reads at hydration time to build
    # iframes, image sources, or WebSocket endpoints. Those live inside
    # <script> tags and are invisible to the HTML-attribute regexes above.
    # Without this pass such a URL stays raw and unproxied; the browser then
    # points straight at the real origin → refused/blocked connection.
    def _script_url_rep(m2):
        host = m2.group(1)
        # Don't default empty tail to "/" — that produces double-slash URLs
        # (http://localhost:PORT/__s2l_ext__/host//path) when the SPA later
        # concatenates its own path. Keep it empty so concatenation works.
        tail = m2.group(2) or ""
        if any(bad in host for bad in _s2l_state.CDN_BLOCK):
            return m2.group(0)
        r = _s2l_state._proxy_target(host, tail)
        return r if r else m2.group(0)

    def _script_block_rep(mb):
        open_tag = mb.group(1)
        body = mb.group(2)
        # Skip JSON-LD and static JSON data blocks — their URLs are identifiers
        # (e.g. "@type": "https://schema.org/Person"), not fetchable resources.
        # Rewriting them breaks SEO/structured data and framework hydration state.
        _type_m = re.search(r'\btype\s*=\s*["\']([^"\']+)["\']', open_tag, re.IGNORECASE)
        if _type_m:
            _script_type = _type_m.group(1).strip().lower()
            if _script_type in ("application/ld+json", "application/json",
                                "importmap", "application/graphql"):
                return mb.group(0)
        # Skip script blocks that look like JSON (first non-whitespace char is {).
        # Many SPAs emit untyped <script>window.__INITIAL_STATE__ = {...}</script>
        # whose URLs are identifiers, not fetchable resources. Rewriting them
        # can cause hydration mismatches.
        _stripped = body.lstrip()
        if _stripped and _stripped[0] == "{":
            return mb.group(0)
        # Skip webpack/module-federation chunks and minified bundle fragments.
        # These contain URLs as object KEYS (cache lookups, integrity maps),
        # and rewriting them produces a key mismatch → silent asset load failure.
        # Heuristics: minified code (long lines), IIFE wrappers, webpack runtime.
        if len(body) > 2000 and (
                "(function()" in body[:200] or "!function(" in body[:200]
                or "webpackChunk" in body[:500] or "__webpack_require__" in body[:2000]):
            return mb.group(0)
        # Skip template-script fragments that embed URLs as React/Vue JSX
        # string identifiers — these would be picked up by the URL regex but
        # are NOT fetchable resources, they're props/state.
        if "data-react-" in body[:500] or "__NEXT_DATA__" in body[:500]:
            return mb.group(0)
        # Skip Next.js App Router "Flight" (React Server Components) streaming
        # payloads — self.__next_f.push([...]) calls. These carry the RSC wire
        # format, where each row is length-prefixed as "id:T<hex-byte-len>,<text>".
        # The pushed text also duplicates page CSS/JSON verbatim (for streaming
        # hydration), so it commonly contains bare URLs (font/background url(),
        # href/src values inside serialized element props) that the regex below
        # would happily rewrite. But rewriting changes the BYTE LENGTH of that
        # text without updating the declared hex length that precedes it,
        # desynchronizing the rest of the stream's framing. React's Flight
        # client then fails to resolve some chunk and throws "Uncaught Error:
        # Connection closed." once the stream ends — the page renders a blank
        # body even though the network tab shows the document loaded fine.
        if "self.__next_f" in body[:500] or "__next_f.push" in body[:500]:
            return mb.group(0)
        return mb.group(1) + re.sub(
            r'https?://([a-zA-Z0-9\-._]{2,}(?::\d+)?)((?:/[^\s"\'\\<>`]*)?)',
            _script_url_rep,
            body,
        ) + mb.group(3)

    html = re.sub(
        r'(<script(?:\s[^>]*)?>)(.*?)(</script\s*>)',
        _script_block_rep,
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )

    # Restore the protected style attribute bodies (placeholders survive every
    # pass above untouched; a placeholder inside a tag that a strip-pass
    # removed simply no longer exists — replace() is a no-op there).
    for _i, _body in enumerate(_style_bodies):
        html = html.replace(f"\x00S2LSTYLE{_i}\x00", _body)

    return html.encode("utf-8")


def rewrite_doc_relative(data: bytes, doc_host: str, html_doc: bool = True) -> bytes:
    """Rewrite ROOT-RELATIVE URLs in a document served from an EXTERNAL origin.

    A document fetched through /__s2l_ext__/<host>/… (or a MULTIPORT
    mini-server) has its *origin* moved to the proxy, so a root-relative
    reference like href="/assets/app.css" — which the real site resolves
    against the DOCUMENT's own host — silently changes meaning in the
    mirror: the browser resolves it against the proxy origin and it lands
    on the MAIN site route (typically its 404 JSON body, served with
    nosniff → the browser logs a MIME mismatch and drops the resource).
    The Discord.com widget is the canonical case: its HTML ships
    <link href="/assets/…css"> / <script src="/assets/…js"> and its CSS
    ships url(/assets/…woff2) — all root-relative.

    This pass re-points those URLs at the document's own host through
    _proxy_target (dedicated MULTIPORT port when registered, the catch-all
    __s2l_ext__ prefix otherwise). Only genuine root-relative values are
    touched:

      html_doc=True  → src/href/poster/data-src/data-href/action attribute
                       values + srcset candidates
      html_doc=False → standalone-CSS url() tokens

    Protocol-relative (//host) values are left for the absolute passes;
    already-proxied values are skipped (_proxy_target's idempotency guard
    plus the (?!/) exclusion); a doc_host under DONT_REWRITE disables the
    pass entirely — its resources go DIRECT from the browser by contract.
    """
    if (not _s2l_state.PROXY_CDN or not data or not doc_host
            or _s2l_state._is_dont_rewrite_host(doc_host)):
        return data
    try:
        text = data.decode("utf-8", "ignore")
    except Exception:
        return data

    if html_doc:
        def _attr_root(m: re.Match) -> str:
            r = _s2l_state._proxy_target(doc_host, m.group(3))
            if not r:
                return m.group(0)
            return f"{m.group(1)}{m.group(2)}{r}{m.group(4)}"
        # (?!/) excludes protocol-relative //host values — those belong to
        # the absolute passes. [^"'<>]* spans the value up to the closing
        # quote, keeping any query string / fragment intact.
        text = re.sub(
            r'((?<![A-Za-z0-9_$.])(?:src|href|poster|data-src|data-href|action)\s*=\s*)(["\'])(/(?!/)[^"\'<>]*)(["\'])',
            _attr_root, text, flags=re.IGNORECASE)

        def _ss_root(m: re.Match) -> str:
            parts = []
            for e in re.split(r',\s+', m.group(2).strip()):
                e = e.strip()
                if not e:
                    continue
                bits = e.split(None, 1)
                url, desc = bits[0], ((" " + bits[1]) if len(bits) > 1 else "")
                if url.startswith("/") and not url.startswith("//"):
                    r = _s2l_state._proxy_target(doc_host, url)
                    if r:
                        url = r
                parts.append(url + desc)
            return m.group(1) + ", ".join(parts) + m.group(3)
        text = re.sub(r'(srcset\s*=\s*["\'])([^"\']+)(["\'])',
                      _ss_root, text, flags=re.IGNORECASE)
    else:
        def _css_root_q(m: re.Match) -> str:
            r = _s2l_state._proxy_target(doc_host, m.group(2))
            return f'url("{r}")' if r else m.group(0)
        text = re.sub(
            r'url\(\s*(["\'])(/(?!/)[^"\']*)\1\s*\)',
            _css_root_q, text, flags=re.IGNORECASE)

        def _css_root_u(m: re.Match) -> str:
            r = _s2l_state._proxy_target(doc_host, m.group(1))
            return f'url("{r}")' if r else m.group(0)
        text = re.sub(
            r'url\(\s*(/(?!/)[^()\s]*)\s*\)',
            _css_root_u, text, flags=re.IGNORECASE)

    return text.encode("utf-8")
