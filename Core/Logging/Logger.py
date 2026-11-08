"""Site2Local: logging and lolcat output."""
from __future__ import annotations

from colorama import Fore
from colorama import Style
import sys
import time

from ... import Runtime as _s2l_state


def log(msg: str, level: str = "INFO") -> None:
    ts    = time.strftime("%H:%M:%S")
    color = _s2l_state._LEVEL_COLOR.get(level, Fore.WHITE)
    tag   = f"{color}[{level}]{Style.RESET_ALL}"
    line  = f"{Style.DIM}{ts}{Style.RESET_ALL} {tag} {msg}"
    with _s2l_state._log_lock:
        if _s2l_state.RAINBOW_LOGS:
            line = _s2l_state._lolcat_real(line) or _s2l_state._lolcat_internal(line)
        # When a tqdm progress bar is active (SCAN_PATHS), route ALL log
        # output through tqdm.write() so log lines from any thread appear
        # ABOVE the bar instead of corrupting it mid-line. Without this,
        # log() uses print() (stdout) while tqdm writes to stderr, and the
        # two streams interleave in the terminal, producing garbled output
        # like "[INFO] Listening...  test.txt  12%|██▌  | 555/4751".
        if _s2l_state._TQDM_OK and _s2l_state._active_scan_pbar is not None:
            _s2l_state._tqdm.write(line, file=sys.stderr)
        else:
            print(line)
