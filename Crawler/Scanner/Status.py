"""Site2Local: hidden-path scanner."""
from __future__ import annotations

from colorama import Fore
from colorama import Style

from ... import Runtime as _s2l_state


def _scan_paths_summary() -> tuple[int, int]:
    """Return (n_wordlists, n_total_paths) for the banner."""
    wls = _s2l_state._resolve_wordlists()
    return (len(wls), sum(len(p) for _, p in wls))


def _parse_status_csv(raw: str) -> tuple[list[int], list[str]]:
    """Parse a comma-separated list of status codes.

    Returns (valid_codes, invalid_tokens). The caller uses invalid_tokens to
    decide whether to re-prompt the user — a single typo should NOT silently
    drop a status from the filter list.
    """
    valid: list[int] = []
    invalid: list[str] = []
    if not raw:
        return valid, invalid
    for tok in raw.replace(";", ",").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            n = int(tok)
            if 100 <= n <= 599:
                valid.append(n)
            else:
                invalid.append(tok)
        except ValueError:
            invalid.append(tok)
    return valid, invalid


def _prompt_scan_status_filter() -> None:
    """Interactive prompt that runs ONCE, in the main thread, before the
    scanner daemon starts. Sets _SCAN_BLOCK_STATUSES and _SCAN_ONLY_STATUS
    based on user input.

    Designed to be self-explanatory: prints the full list of known statuses,
    accepts either "block these" (CSV) or "show only this one", and validates
    every token. Re-prompts on any invalid input, including the edge case of
    trying to block EVERY known status (which would produce an empty log).

    This MUST run in the main thread because it uses input() — running it in
    the scanner daemon thread would race with concurrent stdin reads. Calling
    it before the scanner starts sidesteps the issue entirely.
    """

    # Reset to defaults each time (idempotent — if called again, fresh start)
    _s2l_state._SCAN_BLOCK_STATUSES = set()
    _s2l_state._SCAN_ONLY_STATUS = None

    print()
    print(f"{Fore.CYAN}{Style.BRIGHT}══ SCAN_PATHS — status filter ══{Style.RESET_ALL}")
    print()
    print(f"{Fore.YELLOW}Filter HTTP status codes before scanning? "
          f"{Fore.WHITE}(this can prevent log pollution, you can also ignore this by "
          f"just pressing enter with no stuff typed.){Style.RESET_ALL}")
    print()
    # Show the full list of known status codes — the user needs to see the
    # universe of options before typing any. Format as a wrapped, colorized
    # list so it's scannable at a glance.
    status_chips = []
    for sc in _s2l_state._SCAN_KNOWN_STATUSES:
        if sc == 200:
            color = Fore.GREEN
        elif 300 <= sc < 400:
            color = Fore.YELLOW
        elif 400 <= sc < 500:
            color = Fore.RED
        else:
            color = Fore.MAGENTA + Style.BRIGHT
        status_chips.append(f"{color}{sc}{Style.RESET_ALL}")
    # Print as wrapped lines of ~12 chips each
    line_buf: list[str] = []
    line_len = 0
    print(f"{Fore.WHITE}Available HTTP status codes: {Style.RESET_ALL}")
    for chip in status_chips:
        chip_len = len(_s2l_state._ANSI_ESC.sub("", chip))
        if line_len + chip_len + 2 > 80 and line_buf:
            print("  " + "  ".join(line_buf))
            line_buf = [chip]
            line_len = chip_len
        else:
            line_buf.append(chip)
            line_len += chip_len + 2
    if line_buf:
        print("  " + "  ".join(line_buf))
    print()

    # ── Main filter prompt ──────────────────────────────────────────────
    while True:
        try:
            raw = input(f"{Fore.WHITE}Enter status codes to hide "
                        f"{Fore.YELLOW}(e.g. 404,400){Fore.WHITE}\n"
                        f"{Fore.CYAN}> {Style.RESET_ALL}").strip()
        except (EOFError, KeyboardInterrupt):
            # User pressed Ctrl+D / Ctrl+C — treat as "no filter"
            print(f"{Fore.YELLOW}No filter selected — proceeding with full logging.{Style.RESET_ALL}")
            return

        if not raw:
            # Empty input — no filtering, proceed with full logging
            print(f"{Fore.GREEN}OK, no status filter set — all status codes will be logged.{Style.RESET_ALL}")
            return

        valid, invalid = _s2l_state._parse_status_csv(raw)
        if invalid:
            print(f"{Fore.RED}Invalid status code — enter a valid one{Style.RESET_ALL} "
                  f"(bad token(s): {', '.join(invalid)})")
            continue

        if not valid:
            print(f"{Fore.RED}Invalid status code — enter a valid one{Style.RESET_ALL}")
            continue

        # Edge case 1: user tried to block EVERY known status — log would
        # be empty. Re-prompt.
        if set(valid) >= set(_s2l_state._SCAN_KNOWN_STATUSES):
            print(f"{Fore.RED}You selected every status code — try again{Style.RESET_ALL} "
                  f"(blocking every code would produce an empty log)")
            continue

        # Edge case 2: user blocked all but ONE known status — equivalent to
        # "show only this one", but be explicit so the user understands what
        # just happened. This is the user's spec message verbatim.
        unblocked_known = [s for s in _s2l_state._SCAN_KNOWN_STATUSES if s not in set(valid)]
        if len(unblocked_known) == 1:
            only = unblocked_known[0]
            _s2l_state._SCAN_BLOCK_STATUSES = set(valid)
            _s2l_state._SCAN_ONLY_STATUS = only
            print(f"{Fore.GREEN}You selected to block all status codes except {only}, "
                  f"proceeding to scan...{Style.RESET_ALL}")
            return

        # Normal case — set the block list and exit the prompt.
        _s2l_state._SCAN_BLOCK_STATUSES = set(valid)
        _s2l_state._SCAN_ONLY_STATUS = None
        print(f"{Fore.GREEN}OK, hiding {len(valid)} status code(s) from the log: "
              f"{', '.join(str(s) for s in sorted(valid))}{Style.RESET_ALL}")
        # ── Optional: "show only ONE" mode ─────────────────────────────
        try:
            yn = input(f"{Fore.YELLOW}Show only one status code? "
                       f"{Fore.WHITE}(y/N) {Fore.CYAN}> {Style.RESET_ALL}").strip().lower()
        except (EOFError, KeyboardInterrupt):
            yn = ""
        if yn in ("y", "yes"):
            while True:
                try:
                    only_raw = input(f"{Fore.WHITE}Show only: {Style.RESET_ALL}").strip()
                except (EOFError, KeyboardInterrupt):
                    print(f"{Fore.YELLOW}No 'only' filter — using block list only.{Style.RESET_ALL}")
                    break
                try:
                    only = int(only_raw)
                except ValueError:
                    print(f"{Fore.RED}Invalid status code — enter a valid one{Style.RESET_ALL}")
                    continue
                if not (100 <= only <= 599):
                    print(f"{Fore.RED}Invalid status code — enter a valid one{Style.RESET_ALL}")
                    continue
                _s2l_state._SCAN_ONLY_STATUS = only
                # Remove the "only" status from the block set so the two
                # filters aren't contradictory (block 200 + only 200 = confusing).
                # _should_log_status checks only_status FIRST, so functionally
                # this is a no-op — but it keeps the JSON output and the log
                # messages honest about what's actually being filtered.
                _s2l_state._SCAN_BLOCK_STATUSES.discard(only)
                print(f"{Fore.GREEN}OK, showing only HTTP {only} responses.{Style.RESET_ALL}")
                break
        return
