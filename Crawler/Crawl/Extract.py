"""Pure link discovery: HTML attributes, CSS references and quoted URLs."""
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse, urldefrag


_ATTRS = {
    "a": ("href",), "link": ("href",), "script": ("src",),
    "img": ("src", "data-src"), "source": ("src",),
    "video": ("src", "poster"), "audio": ("src",), "track": ("src",),
    "iframe": ("src",), "embed": ("src",), "object": ("data",), "input": ("src",),
}
_QUOTED = r'''"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*' '''.strip()
_CSS_TOKENS = re.compile(
    r'/\*.*?(?:\*/|$)|(?P<url>url\s*\()|@import\s+(?P<import>' + _QUOTED + r')|'
    + _QUOTED, re.I | re.S)
_TEXT_TOKENS = re.compile(r'/\*.*?(?:\*/|$)|//[^\r\n]*|' + _QUOTED, re.S)


def _absolute(value, base):
    value = value.strip()
    if not value or value.startswith("#") or "${" in value:
        return None
    try:
        url = urldefrag(urljoin(base, value))[0]
        parsed = urlparse(url)
        return url if parsed.scheme in ("http", "https") and parsed.hostname else None
    except ValueError:
        return None


def _css_unescape(value):
    return re.sub(r'\\(?:([0-9a-fA-F]{1,6})\s?|([^\r\n])|\r?\n)',
                  lambda m: chr(int(m[1], 16)) if m[1] and 0 < int(m[1], 16) <= 0x10ffff
                  else (m[2] or "") if not m[1] else "\ufffd", value)


def css_urls(text):
    """Ignore comments and unrelated strings; retain CSS escapes and queries."""
    position = 0
    while match := _CSS_TOKENS.search(text, position):
        position = match.end()
        if match.group("import"):
            yield _css_unescape(match.group("import")[1:-1])
        elif match.group("url"):
            start = position
            quote = None
            depth = 1
            escaped = False
            while position < len(text):
                ch = text[position]
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif quote:
                    if ch == quote:
                        quote = None
                elif ch in "\"'":
                    quote = ch
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        value = text[start:position].strip()
                        if len(value) >= 2 and value[0] in "\"'" and value[-1] == value[0]:
                            value = value[1:-1]
                        yield _css_unescape(value)
                        position += 1
                        break
                position += 1


def srcset_urls(value):
    # Commas inside URLs (data URIs / image transformation paths) are not
    # separators. Candidate descriptors end at the next comma.
    position = 0
    while position < len(value):
        while position < len(value) and (value[position].isspace() or value[position] == ","):
            position += 1
        start = position
        while position < len(value) and not value[position].isspace():
            position += 1
        url = value[start:position]
        if url:
            yield url.rstrip(",")
        if url.endswith(","):
            continue
        while position < len(value) and value[position] != ",":
            position += 1


def text_urls(text):
    """Conservative JS/JSON discovery: complete quoted, non-interpolated URLs."""
    for match in _TEXT_TOKENS.finditer(text):
        token = match.group()
        if token.startswith(("\"", "'")):
            if text[match.end():].lstrip().startswith("+") or text[:match.start()].rstrip().endswith("+"):
                continue  # a concatenated fragment is not a complete URL
            value = token[1:-1].replace("\\/", "/")
            if value.startswith(("https://", "http://", "//", "/", "./", "../")) and "\\" not in value:
                yield value


def extract_links(data, base, content_type, scan_text=False, scan_limit=262144):
    """Return unique (HTTP URL, is_asset) pairs, preserving query strings."""
    text = data.decode("utf-8", "replace")
    ct = content_type.lower()
    found = {}

    def add(value, asset=True):
        url = _absolute(value, base)
        if url:
            found[url] = found.get(url, False) or asset

    if "html" in ct:
        soup = BeautifulSoup(data, "html.parser")
        script_budget = scan_limit
        base_tag = soup.find("base", href=True)
        if base_tag:
            base = _absolute(base_tag["href"], base) or base
        for tag in soup.find_all(True):
            for attribute in _ATTRS.get(tag.name, ()):
                if isinstance(tag.get(attribute), str):
                    add(tag[attribute], tag.name != "a")
            for attribute in ("srcset", "data-srcset"):
                if tag.name in ("img", "source") and isinstance(tag.get(attribute), str):
                    for value in srcset_urls(tag[attribute]):
                        add(value)
            for value in css_urls(tag.get("style", "")):
                add(value)
            if tag.name == "style":
                for value in css_urls(tag.get_text()):
                    add(value)
            if scan_text and script_budget > 0 and tag.name == "script" and not tag.get("src"):
                script = tag.get_text()[:script_budget]
                script_budget -= len(script)
                for value in text_urls(script):
                    add(value)
    elif "css" in ct:
        for value in css_urls(text):
            add(value)
    elif scan_text:
        for value in text_urls(data[:scan_limit].decode("utf-8", "replace")):
            add(value)
    return list(found.items())
