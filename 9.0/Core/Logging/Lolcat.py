"""Site2Local: logging and lolcat output."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
import subprocess
import sys
import time

from ... import Runtime as _s2l_state


def _lolcat_internal(text: str) -> str:
    """Palette-cycling fallback used when the real `lolcat` binary isn't
    installed or doesn't behave the way we expect it to."""
    plain = _s2l_state._ANSI_ESC.sub("", text)
    out, i = [], 0
    for ch in plain:
        if ch.isspace():
            out.append(ch)
        else:
            out.append(_s2l_state._LOLCAT_PALETTE[i % len(_s2l_state._LOLCAT_PALETTE)] + ch)
            i += 1
    return "".join(out) + Style.RESET_ALL


def _probe_lolcat() -> list | bool:
    """One-time detection of which lolcat CLI dialect is installed.

    Different distros/ecosystems ship different `lolcat` implementations
    (the original Ruby gem, and Python/Rust/Go rewrites) with different flags
    for forcing color output when stdout isn't a TTY — which it never is here,
    since every invocation goes through subprocess. Probe once at first use,
    remember whichever flag combination actually produced ANSI-colored output,
    and never re-probe (or re-pay the probe cost) for the rest of the run.
    """

    with _s2l_state._lolcat_probe_lock:
        if _s2l_state._lolcat_mode is not None:
            return _s2l_state._lolcat_mode
        if not _s2l_state._LOLCAT_BIN:
            _s2l_state._lolcat_mode = False
            return False
        for args in (["-f"], ["--force"], []):
            try:
                p = subprocess.run([_s2l_state._LOLCAT_BIN, *args], input=b"s2l\n",
                                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                    timeout=1.5, check=False)
                if p.returncode == 0 and p.stdout and b"\x1b[" in p.stdout:
                    _s2l_state._lolcat_mode = args
                    return args
            except Exception:
                continue
        _s2l_state._lolcat_mode = False
        return False


def _lolcat_real(text: str) -> str | None:
    """Pipe one line through the real lolcat binary. Returns None (never
    raises) on any failure so the caller can fall back to the internal
    palette — a broken pipe here must never take down logging itself."""
    mode = _s2l_state._probe_lolcat()
    if mode is False:
        return None
    try:
        p = subprocess.run([_s2l_state._LOLCAT_BIN, *mode], input=text.encode("utf-8", "replace"),
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=0.5, check=False)
        if p.returncode == 0 and p.stdout:
            out = p.stdout.decode("utf-8", "replace")
            # Some lolcat builds wrap at a default terminal width (commonly 80
            # cols) when stdout isn't a TTY, which would split one log line
            # into several and break the one-line-per-entry log format. Treat
            # that as a miss rather than print a mangled multi-line entry.
            if out.count("\n") > 1:
                return None
            return out.rstrip("\n")
    except Exception:
        pass
    return None
