"""Site2Local: gui."""
from __future__ import annotations

from urllib.parse import urlparse
import codecs
import json
import queue
import re
import sys

from .. import Runtime as _s2l_state
from ..Rewriting.HTML.ServiceWorker import _strip_s2l_injector


def _launch_hook_gui() -> None:
    """
    Launch the PyQt6 GUI. Must be called from the MAIN thread.
    When HOOK_GUI=True, Flask moves to a daemon thread and this blocks main.

    Theme: Qt default — `app.setStyle("Fusion")` with the platform's default
    palette (no custom QPalette, no custom QSS). Native Fusion look on every
    platform (Linux/macOS/Windows). Semantic colors (status indicators, error
    red, save green) are taken from the active palette so they stay legible
    regardless of whether the user runs a light or dark system theme.

    Two tabs (Traffic/Hooks, WebSocket Hooks), live queue polling via
    QTimer, the same hook schema on disk, and the same keyboard shortcuts.
    """
    try:
        from PyQt6.QtCore import (
            Qt, QTimer, qFormatLogMessage, qInstallMessageHandler,
        )
        from PyQt6.QtGui import (
            QPalette, QColor, QFont, QKeySequence, QShortcut,
            QTextCursor, QTextCharFormat,
        )
        from PyQt6.QtWidgets import (
            QApplication, QMainWindow, QWidget, QTabWidget,
            QTreeWidget, QTreeWidgetItem, QPlainTextEdit, QLineEdit, QComboBox,
            QCheckBox, QPushButton, QLabel, QSplitter, QScrollArea, QGridLayout,
            QVBoxLayout, QHBoxLayout, QMessageBox, QInputDialog,
            QFileDialog, QHeaderView, QAbstractItemView, QTextEdit, QSizePolicy,
        )
    except ImportError:
        _s2l_state.log("PyQt6 not available — HOOK_GUI ignored "
            "(pip install PyQt6 --break-system-packages)", "WARN")
        return

    def _gui_main() -> None:

        app = QApplication.instance() or QApplication(sys.argv)
        app.setStyle("Fusion")

        def _qt_message_handler(message_type, context, message):
            if (_s2l_state._qt_shutdown_requested.is_set()
                    and message.startswith("QThreadStorage: entry ")
                    and " destroyed before end of thread " in message):
                return
            if _s2l_state._s2l_qt_previous_message_handler is not None:
                _s2l_state._s2l_qt_previous_message_handler(
                    message_type, context, message)
            else:
                sys.stderr.write(
                    qFormatLogMessage(message_type, context, message) + "\n")
                sys.stderr.flush()

        _s2l_state._s2l_qt_message_handler = _qt_message_handler
        _s2l_state._s2l_qt_previous_message_handler = qInstallMessageHandler(
            _s2l_state._s2l_qt_message_handler)

        # ── Capture unhandled exceptions in Qt slots ───────────────────────
        # Without this, an exception inside a QTimer slot (e.g. _poll) is
        # printed to stderr but the timer keeps firing — and on Windows the
        # traceback may never flush, leaving the GUI silently empty with no
        # clue why. Route every slot exception through log() so it shows up
        # in the same terminal window as the rest of s2l's output.
        import traceback as _tb_mod
        def _s2l_excepthook(exc_type, exc_value, exc_tb):
            try:
                msg = "".join(_tb_mod.format_exception(exc_type, exc_value, exc_tb))
                _s2l_state.log(f"GUI slot raised {exc_type.__name__}: {exc_value}\n{msg}", "ERROR")
            except Exception:
                # If even logging fails, fall back to the default hook.
                sys.__excepthook__(exc_type, exc_value, exc_tb)
        sys.excepthook = _s2l_excepthook
        # PyQt6 6.5+ honors sys.excepthook for slot exceptions. For older
        # versions we also wrap each slot body in try/except (done below).

        # ── Theme: Qt default (Fusion style + platform palette) ──────────────
        # We don't override the palette or the stylesheet — Fusion draws
        # everything with whatever palette the platform shipped (light on
        # Windows/macOS defaults, dark on KDE/GNOME dark themes). The color
        # constants below are derived from `app.palette()` so the explicit
        # setStyleSheet calls scattered across the builder functions still
        # produce colors consistent with the surrounding Fusion widgets.
        pal = app.palette()
        _PNL      = pal.color(QPalette.ColorRole.Base).name()
        _PNL_ALT  = pal.color(QPalette.ColorRole.AlternateBase).name()
        _ACC      = pal.color(QPalette.ColorRole.WindowText).name()
        _FG       = pal.color(QPalette.ColorRole.WindowText).name()
        _DIM      = pal.color(QPalette.ColorRole.PlaceholderText).name() \
                   or pal.color(QPalette.ColorRole.Mid).name()
        _BLUE     = pal.color(QPalette.ColorRole.Highlight).name()
        _GRN      = pal.color(QPalette.ColorRole.Link).name() \
                   or "#2a9c4a"
        _YLW      = "#c67e1a"   # semantic amber — no native Fusion equivalent
        _RED      = "#c64042"   # semantic red — no native Fusion equivalent
        _CYN      = pal.color(QPalette.ColorRole.Highlight).name()
        _PURP     = pal.color(QPalette.ColorRole.LinkVisited).name()
        # Button tints for destructive / affirmative actions. Kept muted so
        # they don't clash with Fusion's flat button look.
        _RED_BG = "#a52a2a"
        _GRN_BG = "#2a5c3a"
        _YLW_HI = "#ccae5a"

        # Monospace fonts — Consolas where available, else a generic mono.
        _mono       = QFont("Consolas", 10)
        _mono_b     = QFont("Consolas", 10);  _mono_b.setBold(True)
        _mono_lg    = QFont("Consolas", 11);  _mono_lg.setBold(True)
        _mono_sm    = QFont("Consolas", 9)
        _mono_sm_b  = QFont("Consolas", 9);   _mono_sm_b.setBold(True)
        _mono_xs    = QFont("Consolas", 8)

        # No custom QSS — let Fusion draw everything with the platform's
        # default look. The color constants above are still used by the
        # explicit setStyleSheet calls in the builder functions below, but
        # they now hold values that match the surrounding Fusion palette.

        win = QMainWindow()
        win.setWindowTitle("S2L — Hook Inspector")

        # ── Responsive sizing: fill 90% of screen ──────────────────────────
        screen = app.primaryScreen()
        if screen is not None:
            geo = screen.availableGeometry()
            w = max(1000, int(geo.width() * 0.90))
            h = max(640,  int(geo.height() * 0.88))
            x = geo.x() + (geo.width() - w) // 2
            y = max(geo.y(), geo.y() + (geo.height() - h) // 2 - 20)
            win.setGeometry(x, y, w, h)
        win.setMinimumSize(900, 560)

        # Central widget + root vertical layout
        central = QWidget(win)
        win.setCentralWidget(central)
        root_lay = QVBoxLayout(central)
        root_lay.setContentsMargins(4, 4, 4, 4)
        root_lay.setSpacing(4)

        # ── Helper builders (mirror the old _btn/_lbl/_entry/_text_box) ─────
        def _btn(parent, text, cmd, font=None):
            b = QPushButton(text, parent)
            b.clicked.connect(cmd)
            b.setFont(font or _mono_sm)
            return b

        def _lbl(parent, text="", font=None, color=None):
            l = QLabel(text, parent)
            l.setFont(font or _mono_sm)
            if color:
                l.setStyleSheet(f"color: {color}; background: transparent;")
            return l

        def _entry(parent, width=0):
            e = QLineEdit(parent)
            e.setFont(_mono)
            if width:
                e.setMaximumWidth(width * 8)  # rough char→px
            return e

        def _text_box(parent, wrap=False, mono=None, readonly=False):
            t = QPlainTextEdit(parent)
            t.setFont(mono or _mono)
            t.setReadOnly(readonly)
            if not wrap:
                t.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)
            return t

        # ── Notebook (tabs) ─────────────────────────────────────────────────
        nb = QTabWidget(central)
        nb.setFont(_mono)
        root_lay.addWidget(nb)

        # Cursor-based case-insensitive find for a QPlainTextEdit.
        # Returns the list of start positions and highlights every match.
        def _find_all(edit: QPlainTextEdit, query: str):
            edit.setExtraSelections([])
            if not query:
                return []
            doc = edit.document()
            positions = []
            cur = QTextCursor(doc)
            cur.movePosition(QTextCursor.MoveOperation.Start)
            ql = len(query)
            while True:
                cur = doc.find(query, cur)
                if cur.isNull():
                    break
                positions.append(cur.selectionStart())
            if positions:
                fmt = QTextCharFormat()
                fmt.setBackground(QColor(_YLW_HI))
                fmt.setForeground(QColor("#000000"))
                sels = []
                for p in positions:
                    sel = QTextEdit.ExtraSelection()
                    sel.cursor = QTextCursor(doc)
                    sel.cursor.setPosition(p)
                    sel.cursor.setPosition(p + ql, QTextCursor.MoveMode.KeepAnchor)
                    sel.format = fmt
                    sels.append(sel)
                edit.setExtraSelections(sels)
            return positions

        # ════════════════════════════════════════════════════════════════════
        # TAB 1: Traffic + Hooks
        # ════════════════════════════════════════════════════════════════════
        tab_traffic = QWidget()
        nb.addTab(tab_traffic, "  Traffic / Hooks")

        # Vertical split: top (traffic + editor) | bottom (active hooks)
        v_split = QSplitter(Qt.Orientation.Vertical, tab_traffic)
        v_split.setChildrenCollapsible(False)
        top_area = QWidget()
        bot_area = QWidget()
        bot_area.setStyleSheet(f"background: {_PNL};")
        top_lay = QVBoxLayout(top_area);  top_lay.setContentsMargins(0, 0, 0, 0)
        bot_lay = QVBoxLayout(bot_area); bot_lay.setContentsMargins(0, 0, 0, 0)
        v_split.addWidget(top_area)
        v_split.addWidget(bot_area)
        v_split.setStretchFactor(0, 6)
        v_split.setStretchFactor(1, 1)
        v_split.setSizes([600, 120])

        tv_lay = QVBoxLayout(tab_traffic); tv_lay.setContentsMargins(0, 0, 0, 0)
        tv_lay.addWidget(v_split)

        # Horizontal split inside top_area: left (traffic log) | right (body editor)
        h_split = QSplitter(Qt.Orientation.Horizontal, top_area)
        h_split.setChildrenCollapsible(False)
        left  = QWidget(); left.setStyleSheet(f"background: {_PNL};")
        right = QWidget(); right.setStyleSheet(f"background: {_PNL};")
        left_lay  = QVBoxLayout(left);  left_lay.setContentsMargins(4, 4, 4, 4);  left_lay.setSpacing(2)
        right_lay = QVBoxLayout(right); right_lay.setContentsMargins(4, 4, 4, 4); right_lay.setSpacing(2)
        h_split.addWidget(left)
        h_split.addWidget(right)
        h_split.setStretchFactor(0, 55)
        h_split.setStretchFactor(1, 45)
        h_split.setSizes([560, 420])
        top_lay.addWidget(h_split)

        # ── LEFT: traffic log ───────────────────────────────────────────────
        left_lay.addWidget(_lbl(left, "Traffic Log", font=_mono_lg, color=_ACC))

        cols = ["ts", "method", "origin", "path", "status", "web_type", "ct", "size"]
        labels = ["Time", "Method", "Origin", "Path", "Status", "Web Type", "File Type", "Size"]
        widths = {"ts": 58, "method": 50, "origin": 110, "status": 44,
                  "web_type": 72, "ct": 100, "size": 56}
        tree = QTreeWidget(left)
        tree.setColumnCount(len(cols))
        tree.setHeaderLabels(labels)
        tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        tree.setRootIsDecorated(False)
        tree.setAlternatingRowColors(False)
        tree.setFont(_mono)
        hdr = tree.header()
        hdr.setMinimumSectionSize(40)
        for i, c in enumerate(cols):
            if c == "path":
                hdr.setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)
            else:
                hdr.setSectionResizeMode(i, QHeaderView.ResizeMode.Interactive)
                if c in widths:
                    tree.setColumnWidth(i, widths[c])
        left_lay.addWidget(tree, 1)

        ctrl = QWidget(left); ctrl.setStyleSheet(f"background: {_PNL};")
        ctrl_lay = QHBoxLayout(ctrl); ctrl_lay.setContentsMargins(8, 0, 8, 0); ctrl_lay.setSpacing(6)
        left_lay.addWidget(ctrl)

        auto_scroll_cb = QCheckBox("Auto-scroll", ctrl); auto_scroll_cb.setChecked(True)
        paused_cb     = QCheckBox("Pause", ctrl)
        ctrl_lay.addWidget(auto_scroll_cb)
        ctrl_lay.addWidget(paused_cb)

        def _clear_tree():
            tree.clear()
            _row_data.clear()
        ctrl_lay.addWidget(_btn(ctrl, "Clear", _clear_tree))
        ctrl_lay.addStretch(1)

        # ── Traffic Ctrl+F search bar (hidden until Ctrl+F) ──────────────────
        search_frame = QWidget(left); search_frame.setStyleSheet(f"background: {_PNL};")
        sf_lay = QHBoxLayout(search_frame); sf_lay.setContentsMargins(4, 0, 4, 0); sf_lay.setSpacing(4)
        sf_lay.addWidget(_lbl(search_frame, "Find:", color=_DIM))
        traffic_search_edit = _entry(search_frame)
        sf_lay.addWidget(traffic_search_edit, 1)
        search_frame.setVisible(False)
        left_lay.addWidget(search_frame)

        _row_data: dict[str, dict] = {}
        _row_counter = [0]   # monotonic iid generator (id() can collide after GC)
        _MAX_ROWS = 800

        def _traffic_search_apply(*_):
            q = traffic_search_edit.text().lower()
            for i in range(tree.topLevelItemCount()):
                item = tree.topLevelItem(i)
                iid = item.data(0, Qt.ItemDataRole.UserRole)
                d = _row_data.get(iid, {})
                visible = (not q or q in str(d.get("path", "")).lower()
                           or q in str(d.get("status", ""))
                           or q in str(d.get("ct", "")).lower()
                           or q in str(d.get("method", "")).lower())
                item.setHidden(not visible)

        traffic_search_edit.textChanged.connect(_traffic_search_apply)

        def _traffic_search_close():
            traffic_search_edit.clear()
            search_frame.setVisible(False)
            tree.setFocus()

        def _traffic_search_show(*_):
            search_frame.setVisible(True)
            traffic_search_edit.setFocus()
        _sc1 = QShortcut(QKeySequence("Ctrl+F"), win)
        _sc1.activated.connect(_traffic_search_show)
        _sc2 = QShortcut(QKeySequence("Escape"), traffic_search_edit)
        _sc2.setContext(Qt.ShortcutContext.WidgetShortcut)
        _sc2.activated.connect(_traffic_search_close)

        # ── RIGHT: body editor ──────────────────────────────────────────────
        right_lay.addWidget(_lbl(right, "Body Editor", font=_mono_lg, color=_ACC))
        info_lbl = _lbl(right, "← Select a request", color=_ACC)
        info_lbl.setMinimumWidth(0)
        info_lbl.setSizePolicy(
            QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        right_lay.addWidget(info_lbl)

        body_box = _text_box(right, wrap=True)
        # Size policy Ignored: the layout must NEVER derive this pane's size
        # from the document. QPlainTextEdit's sizeHint scales with content, so
        # loading a large body would push the QSplitter around and grow the
        # editor pane until it swallows the Traffic Log. With Ignored, pane
        # size is controlled ONLY by the splitter handle (i.e. the user).
        _editor_sp = QSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)
        _editor_sp.setHorizontalStretch(0)
        _editor_sp.setVerticalStretch(0)
        body_box.setSizePolicy(_editor_sp)
        body_box.setMinimumHeight(60)
        right_lay.addWidget(body_box, 1)

        # Body editor Ctrl+F find bar (hidden until Ctrl+F in body_box)
        body_find_frame = QWidget(right); body_find_frame.setStyleSheet(f"background: {_PNL};")
        bf_lay = QHBoxLayout(body_find_frame); bf_lay.setContentsMargins(0, 0, 0, 0); bf_lay.setSpacing(4)
        bf_lay.addWidget(_lbl(body_find_frame, "Find:", color=_DIM))
        body_find_edit = _entry(body_find_frame)
        bf_lay.addWidget(body_find_edit, 1)
        body_find_match_lbl = _lbl(body_find_frame, "", color=_DIM)
        bf_lay.addWidget(body_find_match_lbl)
        body_find_frame.setVisible(False)
        right_lay.addWidget(body_find_frame)

        _body_find_positions: list = []
        _body_find_idx = [0]

        def _body_find_apply(*_):
            q = body_find_edit.text()
            _body_find_positions.clear()
            _body_find_positions.extend(_find_all(body_box, q))
            n = len(_body_find_positions)
            body_find_match_lbl.setText(f"{n} match{'es' if n != 1 else ''}")
            if _body_find_positions:
                _body_find_idx[0] = 0
                cur = QTextCursor(body_box.document())
                cur.setPosition(_body_find_positions[0])
                body_box.setTextCursor(cur)

        def _body_find_next():
            if not _body_find_positions:
                return
            _body_find_idx[0] = (_body_find_idx[0] + 1) % len(_body_find_positions)
            cur = QTextCursor(body_box.document())
            cur.setPosition(_body_find_positions[_body_find_idx[0]])
            body_box.setTextCursor(cur)

        def _body_find_prev():
            if not _body_find_positions:
                return
            _body_find_idx[0] = (_body_find_idx[0] - 1) % len(_body_find_positions)
            cur = QTextCursor(body_box.document())
            cur.setPosition(_body_find_positions[_body_find_idx[0]])
            body_box.setTextCursor(cur)

        def _body_find_close():
            body_box.setExtraSelections([])
            body_find_frame.setVisible(False)
            body_box.setFocus()

        body_find_edit.textChanged.connect(_body_find_apply)
        body_find_edit.returnPressed.connect(_body_find_next)
        _sc3 = QShortcut(QKeySequence("Shift+Return"), body_find_edit)
        _sc3.setContext(Qt.ShortcutContext.WidgetShortcut)
        _sc3.activated.connect(_body_find_prev)
        _sc4 = QShortcut(QKeySequence("Escape"), body_find_edit)
        _sc4.setContext(Qt.ShortcutContext.WidgetShortcut)
        _sc4.activated.connect(_body_find_close)

        def _show_body_find(*_):
            body_find_frame.setVisible(True)
            body_find_edit.setFocus()
            sel = body_box.textCursor().selectedText()
            if sel:
                body_find_edit.setText(sel)
                _body_find_apply()
        _sc5 = QShortcut(QKeySequence("Ctrl+F"), body_box)
        _sc5.setContext(Qt.ShortcutContext.WidgetShortcut)
        _sc5.activated.connect(_show_body_find)

        # ── Hook save controls ──────────────────────────────────────────────
        save_frame = QWidget(right); save_frame.setStyleSheet(f"background: {_PNL};")
        sf2 = QVBoxLayout(save_frame); sf2.setContentsMargins(0, 0, 0, 0); sf2.setSpacing(2)
        right_lay.addWidget(save_frame)

        row1 = QWidget(save_frame); r1 = QHBoxLayout(row1); r1.setContentsMargins(0, 0, 0, 0); r1.setSpacing(4)
        row2 = QWidget(save_frame); r2 = QHBoxLayout(row2); r2.setContentsMargins(0, 0, 0, 0); r2.setSpacing(4)
        row3 = QWidget(save_frame); r3 = QHBoxLayout(row3); r3.setContentsMargins(0, 0, 0, 0); r3.setSpacing(4)
        sf2.addWidget(row1); sf2.addWidget(row2); sf2.addWidget(row3)

        r1.addWidget(_lbl(row1, "Name:", color=_ACC))
        name_edit = _entry(row1, width=14); r1.addWidget(name_edit)
        r1.addWidget(_lbl(row1, "Method:", color=_ACC))
        _METHOD_OPTS = ["*", "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"]
        method_combo = QComboBox(row1); method_combo.setEditable(False)
        method_combo.addItems(_METHOD_OPTS); method_combo.setCurrentText("*")
        method_combo.setFont(_mono); method_combo.setMaximumWidth(90)
        r1.addWidget(method_combo)
        r1.addWidget(_lbl(row1, "Status:", color=_ACC))
        _STATUS_OPTS = ["*", "200", "201", "204", "301", "302", "304",
                        "400", "401", "403", "404", "405", "419", "422",
                        "500", "502", "503"]
        status_combo = QComboBox(row1); status_combo.setEditable(False)
        status_combo.addItems(_STATUS_OPTS); status_combo.setCurrentText("*")
        status_combo.setFont(_mono); status_combo.setMaximumWidth(80)
        r1.addWidget(status_combo)
        r1.addStretch(1)

        r2.addWidget(_lbl(row2, "Pattern URL:", color=_ACC))
        pat_edit = _entry(row2); pat_edit.setText(r".*")
        r2.addWidget(pat_edit, 1)

        r3.addWidget(_lbl(row3, "Origin URL:", color=_ACC))
        origin_edit = _entry(row3); origin_edit.setText("*")
        r3.addWidget(origin_edit, 1)
        r3.addWidget(_lbl(row3, "  (target / * / host)", color=_DIM))

        save_status_lbl = _lbl(right, "", color=_DIM)
        right_lay.addWidget(save_status_lbl)

        def _on_select(*_):
            items = tree.selectedItems()
            if not items:
                return
            item = items[0]
            iid = item.data(0, Qt.ItemDataRole.UserRole)
            data = _row_data.get(iid)
            if not data:
                return

            path_qs = data["path"] + (f"?{data['query']}" if data.get("query") else "")
            _info_origin = data.get("origin", "")
            _info_prefix = f"[{_info_origin}]  " if _info_origin else ""
            _info_wt = data.get("web_type", "")
            _info_wt_s = f"  {_info_wt}" if _info_wt else ""
            info_text = (
                f"{_info_prefix}{data['method']}  {path_qs}  "
                f"[{data['status']}]{_info_wt_s}  {data['ct']}")
            info_lbl.setText(info_text)
            info_lbl.setToolTip(info_text)

            raw_body = data["body"]
            txt, _trunc = _s2l_state._editor_preview(raw_body, data.get("ct", ""))
            body_box.setPlainText(txt)

            method_combo.setCurrentText(data["method"] if data["method"] in _METHOD_OPTS else "*")
            sc = str(data["status"])
            status_combo.setCurrentText(sc if sc in _STATUS_OPTS else "*")
            pat_edit.setText("^" + re.escape(data["path"]) + "$")
            _data_origin = data.get("origin", "")
            origin_edit.setText(_data_origin if _data_origin else "*")
            if not name_edit.text().strip():
                segs = [s for s in data["path"].split("/") if s]
                name_candidate = ""
                for seg in reversed(segs):
                    clean = seg.split("=")[-1]
                    if len(clean) > 32 and re.search(r'^[A-Za-z0-9._-]+$', clean):
                        continue
                    name_candidate = clean[:28]
                    break
                if not name_candidate and segs:
                    name_candidate = "_".join(s[:12] for s in segs[-2:])[:28]
                if name_candidate:
                    name_edit.setText(name_candidate)

        tree.itemSelectionChanged.connect(_on_select)

        # ── Toolbar: Copy as cURL + Export/Import hooks ─────────────────────
        toolbar = QWidget(right); toolbar.setStyleSheet(f"background: {_PNL};")
        tb_lay = QHBoxLayout(toolbar); tb_lay.setContentsMargins(0, 0, 0, 0); tb_lay.setSpacing(4)
        right_lay.addWidget(toolbar)

        def _copy_as_curl():
            items = tree.selectedItems()
            if not items:
                return
            iid = items[0].data(0, Qt.ItemDataRole.UserRole)
            data = _row_data.get(iid)
            if not data:
                return
            url = f"http://localhost:{_s2l_state.PORT}{data['path']}"
            if data.get("query"):
                url += f"?{data['query']}"
            method = data["method"]
            parts = [f"curl -X {method}"]
            for k, v in (data.get("req_headers") or {}).items():
                kl = k.lower()
                if kl in ("host", "content-length", "transfer-encoding"):
                    continue
                safe_v = str(v).replace("'", "'\\''")
                parts.append(f"  -H '{k}: {safe_v}'")
            if data.get("body") and method not in _s2l_state._SAFE_METHODS:
                body_str = data["body"].decode("utf-8", errors="replace").replace("'", "'\\''")
                parts.append(f"  -d '{body_str}'")
            parts.append(f"  '{url}'")
            curl_str = " \\\n".join(parts)
            QApplication.clipboard().setText(curl_str)
            save_status_lbl.setText("cURL command copied to clipboard.")
            save_status_lbl.setStyleSheet(f"color: {_GRN}; background: transparent;")

        def _copy_req_headers():
            items = tree.selectedItems()
            if not items:
                save_status_lbl.setText("Select a request first.")
                save_status_lbl.setStyleSheet(f"color: {_RED}; background: transparent;")
                return
            iid = items[0].data(0, Qt.ItemDataRole.UserRole)
            data = _row_data.get(iid)
            if not data:
                return
            hdrs = data.get("req_headers") or {}
            if not hdrs:
                save_status_lbl.setText("No request headers captured for this entry.")
                save_status_lbl.setStyleSheet(f"color: {_RED}; background: transparent;")
                return
            lines = []
            path_qs = data["path"] + (f"?{data['query']}" if data.get("query") else "")
            lines.append(f"{data['method']} {path_qs}")
            lines.append("")
            for k, v in hdrs.items():
                lines.append(f"{k}: {v}")
            QApplication.clipboard().setText("\n".join(lines))
            save_status_lbl.setText(f"Request headers copied to clipboard ({len(hdrs)} headers).")
            save_status_lbl.setStyleSheet(f"color: {_GRN}; background: transparent;")

        def _export_hooks():
            fp, _ = QFileDialog.getSaveFileName(
                win, "Export Hooks", f"{_s2l_state.SITE_NAME}_hooks.json",
                "JSON (*.json);;All (*)")
            if not fp:
                return
            with _s2l_state._gui_hooks_lock:
                export_data = [
                    {k: v for k, v in h.items() if k != "body_bytes"}
                    | {"body": h["body_bytes"].decode("utf-8", errors="replace")}
                    for h in _s2l_state._gui_hooks
                ]
            with open(fp, "w", encoding="utf-8") as f:
                json.dump(export_data, f, indent=2)
            save_status_lbl.setText(f"Exported {len(export_data)} hook(s) to disk.")
            save_status_lbl.setStyleSheet(f"color: {_GRN}; background: transparent;")

        def _import_hooks():
            fp, _ = QFileDialog.getOpenFileName(win, "Import Hooks", "", "JSON (*.json);;All (*)")
            if not fp:
                return
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    items = json.load(f)
                imported = 0
                for item in items:
                    hook = {
                        "name":       item.get("name", "imported"),
                        "method":     item.get("method", "*"),
                        "status":     int(item.get("status", 200)),
                        "pattern":    item.get("pattern", ".*"),
                        "origin_url": item.get("origin_url", item.get("origin", "*")),
                        "body_bytes": item.get("body", "").encode("utf-8"),
                        "enabled":    item.get("enabled", True),
                    }
                    with _s2l_state._gui_hooks_lock:
                        names = [h["name"] for h in _s2l_state._gui_hooks]
                        if hook["name"] not in names:
                            _s2l_state._gui_hooks.append(hook)
                            imported += 1
                _render_hook_rows()
                save_status_lbl.setText(f"Imported {imported} hook(s) from disk.")
                save_status_lbl.setStyleSheet(f"color: {_GRN}; background: transparent;")
            except Exception as e:
                QMessageBox.critical(win, "S2L", f"Import failed: {e}")

        tb_lay.addWidget(_btn(toolbar, "Copy as cURL", _copy_as_curl, font=_mono_sm))
        tb_lay.addWidget(_btn(toolbar, "Copy Request Headers", _copy_req_headers, font=_mono_sm))
        tb_lay.addWidget(_btn(toolbar, "Export Hooks", _export_hooks, font=_mono_sm))
        tb_lay.addWidget(_btn(toolbar, "Import Hooks", _import_hooks, font=_mono_sm))
        tb_lay.addStretch(1)

        # ── BOTTOM: Active Hooks ───────────────────────────────────────────
        bot_lay.addWidget(_lbl(bot_area, "Active Hooks", font=_mono_lg, color=_ACC))

        hooks_scroll = QScrollArea(bot_area)
        hooks_scroll.setWidgetResizable(True)
        hooks_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        hooks_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        hooks_inner = QWidget()
        hooks_inner.setStyleSheet(f"background: {_PNL};")
        hooks_grid = QGridLayout(hooks_inner)
        hooks_grid.setContentsMargins(8, 4, 8, 4)
        hooks_grid.setSpacing(4)
        hooks_scroll.setWidget(hooks_inner)
        bot_lay.addWidget(hooks_scroll, 1)

        _HOOK_COLS   = ("Name", "Method", "Status", "Origin", "Pattern", "On", "", "")
        _HOOK_WIDTHS = (16, 7, 6, 18, 28, 4, 4, 4)

        def _render_hook_rows():
            # Clear existing rows (keep nothing — re-render from scratch)
            while hooks_grid.count():
                it = hooks_grid.takeAt(0)
                w = it.widget()
                if w is not None:
                    w.deleteLater()
            for col_idx, (col_name, col_w) in enumerate(zip(_HOOK_COLS, _HOOK_WIDTHS)):
                lbl = _lbl(hooks_inner, col_name, font=_mono_sm_b, color=_ACC)
                lbl.setMinimumWidth(col_w * 8)
                hooks_grid.addWidget(lbl, 0, col_idx, Qt.AlignmentFlag.AlignLeft)
            with _s2l_state._gui_hooks_lock:
                hooks_copy = list(_s2l_state._gui_hooks)
            if not hooks_copy:
                hooks_grid.addWidget(_lbl(hooks_inner, "No hooks saved yet.", color=_DIM),
                                      1, 0, 1, 8, Qt.AlignmentFlag.AlignLeft)
                return
            for row_idx, h in enumerate(hooks_copy, start=1):
                row_fg = _FG if h["enabled"] else _DIM
                hooks_grid.addWidget(_lbl(hooks_inner, h["name"], color=row_fg),
                                      row_idx, 0, Qt.AlignmentFlag.AlignLeft)
                hooks_grid.addWidget(_lbl(hooks_inner, h["method"],
                                          color=_YLW if h["enabled"] else _DIM),
                                      row_idx, 1, Qt.AlignmentFlag.AlignLeft)
                hooks_grid.addWidget(_lbl(hooks_inner, str(h["status"]),
                                          color=_GRN if h["enabled"] else _DIM),
                                      row_idx, 2, Qt.AlignmentFlag.AlignLeft)
                _origin_disp = h.get("origin_url", "*") or "*"
                if len(_origin_disp) > 18:
                    _origin_disp = _origin_disp[:17] + "…"
                hooks_grid.addWidget(_lbl(hooks_inner, _origin_disp,
                                          color=_CYN if h["enabled"] else _DIM),
                                      row_idx, 3, Qt.AlignmentFlag.AlignLeft)
                _pat_disp = h["pattern"][:28] + ("…" if len(h["pattern"]) > 28 else "")
                hooks_grid.addWidget(_lbl(hooks_inner, _pat_disp, color=_DIM),
                                      row_idx, 4, Qt.AlignmentFlag.AlignLeft)

                # Enabled toggle
                cb = QCheckBox(hooks_inner)
                cb.setChecked(h["enabled"])

                def _make_toggle(hook_ref, checkbox):
                    def _toggle(_checked):
                        with _s2l_state._gui_hooks_lock:
                            hook_ref["enabled"] = checkbox.isChecked()
                        _hook_save_to_disk(hook_ref)
                        _render_hook_rows()
                    return _toggle
                cb.stateChanged.connect(_make_toggle(h, cb))
                hooks_grid.addWidget(cb, row_idx, 5, Qt.AlignmentFlag.AlignCenter)

                # Edit
                def _make_edit(hook_ref):
                    def _edit():
                        name_edit.setText(hook_ref["name"])
                        method_combo.setCurrentText(hook_ref["method"] if hook_ref["method"] in _METHOD_OPTS else "*")
                        sc = str(hook_ref["status"])
                        status_combo.setCurrentText(sc if sc in _STATUS_OPTS else "200")
                        pat_edit.setText(hook_ref["pattern"])
                        origin_edit.setText(hook_ref.get("origin_url", "*") or "*")
                        try:
                            txt = _strip_s2l_injector(hook_ref["body_bytes"]).decode("utf-8", errors="replace")
                            if hook_ref["body_bytes"].startswith(b"{") or hook_ref["body_bytes"].startswith(b"["):
                                try:
                                    txt = json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
                                except Exception:
                                    pass
                        except Exception:
                            txt = hook_ref["body_bytes"].hex()
                        body_box.setPlainText(txt)
                        save_status_lbl.setText(f"Editing '{hook_ref['name']}'")
                        save_status_lbl.setStyleSheet(f"color: {_YLW}; background: transparent;")
                    return _edit
                _ed_btn = _btn(hooks_inner, "Ed", _make_edit(h), font=_mono_xs)
                _ed_btn.setStyleSheet(f"background: {_GRN_BG}; color: {_GRN}; border: 1px solid #2a4a35;")
                _ed_btn.setMaximumWidth(36)
                hooks_grid.addWidget(_ed_btn,
                                      row_idx, 6, Qt.AlignmentFlag.AlignCenter)

                # Delete
                def _make_del(hook_name):
                    def _del():
                        with _s2l_state._gui_hooks_lock:
                            idx = next((i for i, x in enumerate(_s2l_state._gui_hooks)
                                        if x["name"] == hook_name), None)
                            if idx is not None:
                                _s2l_state._gui_hooks.pop(idx)
                        _hook_delete_from_disk(hook_name)
                        _render_hook_rows()
                        save_status_lbl.setText(f"Deleted '{hook_name}'.")
                        save_status_lbl.setStyleSheet(f"color: {_YLW}; background: transparent;")
                    return _del
                del_btn = _btn(hooks_inner, "X", _make_del(h["name"]), font=_mono_xs)
                del_btn.setStyleSheet(f"background: {_RED_BG}; color: {_RED}; border: 1px solid #5a2a30;")
                del_btn.setMaximumWidth(36)
                hooks_grid.addWidget(del_btn, row_idx, 7, Qt.AlignmentFlag.AlignCenter)

        _render_hook_rows()

        # ── Hook disk persistence — delegates to the shared store factory ──
        _hook_save_to_disk, _hook_delete_from_disk, _hook_load_all = _s2l_state._make_hook_store("MyHooks")

        def _load_hooks_from_disk() -> None:
            """Load previously saved hooks from site_data/MyHooks/{host}/ at startup."""
            for hook in _hook_load_all({"name": "?", "method": "*", "status": 200,
                                         "pattern": ".*", "origin_url": "*",
                                         "enabled": True}):
                hook["status"] = int(hook.get("status", 200) or 0)
                if "origin_url" not in hook:
                    hook["origin_url"] = "*"
                with _s2l_state._gui_hooks_lock:
                    if hook["name"] not in [h["name"] for h in _s2l_state._gui_hooks]:
                        _s2l_state._gui_hooks.append(hook)
                        _s2l_state.log(f"Loaded hook '{hook['name']}' from disk", "HOOK")

        QTimer.singleShot(100, lambda: (_load_hooks_from_disk(), _render_hook_rows()))

        def _save_hook():
            name = name_edit.text().strip()
            if not name:
                QMessageBox.warning(win, "S2L", "Hook name is required.")
                return
            with _s2l_state._gui_hooks_lock:
                existing_names = [h["name"] for h in _s2l_state._gui_hooks]
            if name in existing_names:
                if QMessageBox.question(win, "S2L",
                        f"A hook named '{name}' already exists.\nReplace it?"
                    ) != QMessageBox.StandardButton.Yes:
                    return
                with _s2l_state._gui_hooks_lock:
                    for i, h in enumerate(_s2l_state._gui_hooks):
                        if h["name"] == name:
                            _s2l_state._gui_hooks.pop(i)
                            break
            body_txt = body_box.toPlainText()
            if not body_txt.strip():
                QMessageBox.warning(win, "S2L", "Hook body is empty — nothing to inject.")
                return
            _sv = status_combo.currentText().strip()
            status = 0 if _sv in ("*", "any", "") else (int(_sv) if _sv.isdigit() else None)
            if status is None:
                QMessageBox.critical(win, "S2L", "Status must be a number or *.")
                return
            pat_str = pat_edit.text().strip() or r".*"
            try:
                re.compile(pat_str)
            except re.error as e:
                QMessageBox.critical(win, "S2L", f"Invalid pattern: {e}")
                return
            origin_str = origin_edit.text().strip() or "*"
            hook = {
                "name":       name,
                "method":     method_combo.currentText(),
                "status":     status,
                "pattern":    pat_str,
                "origin_url": origin_str,
                "body_bytes": _strip_s2l_injector(body_txt.encode("utf-8")),
                "enabled":    True,
            }
            with _s2l_state._gui_hooks_lock:
                _s2l_state._gui_hooks.append(hook)
            _hook_save_to_disk(hook)
            _render_hook_rows()
            save_status_lbl.setText(
                f"Saved '{name}'  [{hook['method']}]  {hook['pattern']}")
            save_status_lbl.setStyleSheet(f"color: {_GRN}; background: transparent;")
            _s2l_state.log(f"GUI hook saved: '{name}' [{hook['method']}] {hook['pattern']}", "HOOK")

        def _test_hook():
            pat_str = pat_edit.text().strip()
            mf = method_combo.currentText()
            test_path, ok = QInputDialog.getText(win, "Test Pattern", "Enter a path to test:")
            if not ok or test_path is None:
                return
            try:
                matched_pat = bool(re.search(pat_str, test_path, re.IGNORECASE))
            except re.error as e:
                QMessageBox.critical(win, "S2L", f"Invalid pattern: {e}")
                return
            matched_meth = mf in ("", "*")
            result = "MATCH" if matched_pat else "NO MATCH"
            save_status_lbl.setText(
                f"{result}: pattern={'yes' if matched_pat else 'no'}  "
                f"method={'any' if matched_meth else mf}")
            save_status_lbl.setStyleSheet(
                f"color: {_GRN if matched_pat else _RED}; background: transparent;")

        btn_row2 = QWidget(right); btn_row2.setStyleSheet(f"background: {_PNL};")
        br2 = QHBoxLayout(btn_row2); br2.setContentsMargins(0, 0, 0, 0); br2.setSpacing(4)
        right_lay.addWidget(btn_row2)
        _sb = _btn(btn_row2, "Save Hook", _save_hook, font=_mono_b)
        br2.addWidget(_sb, 1)
        br2.addWidget(_btn(btn_row2, "Test Pattern", _test_hook, font=_mono_sm))

        # ════════════════════════════════════════════════════════════════════
        # TAB 2: WebSocket Hooks
        # ════════════════════════════════════════════════════════════════════
        tab_ws = QWidget()
        nb.addTab(tab_ws, "  WebSocket Hooks")

        ws_v = QSplitter(Qt.Orientation.Vertical, tab_ws); ws_v.setChildrenCollapsible(False)
        ws_top = QWidget(); ws_bot = QWidget(); ws_bot.setStyleSheet(f"background: {_PNL};")
        ws_v.addWidget(ws_top); ws_v.addWidget(ws_bot)
        ws_v.setStretchFactor(0, 6); ws_v.setStretchFactor(1, 1)
        ws_v.setSizes([600, 120])
        ws_v_lay = QVBoxLayout(tab_ws); ws_v_lay.setContentsMargins(0, 0, 0, 0)
        ws_v_lay.addWidget(ws_v)

        ws_h = QSplitter(Qt.Orientation.Horizontal, ws_top); ws_h.setChildrenCollapsible(False)
        ws_left  = QWidget(); ws_left.setStyleSheet(f"background: {_PNL};")
        ws_right = QWidget(); ws_right.setStyleSheet(f"background: {_PNL};")
        ws_h.addWidget(ws_left); ws_h.addWidget(ws_right)
        ws_h.setStretchFactor(0, 50); ws_h.setStretchFactor(1, 50)
        ws_h.setSizes([520, 460])
        ws_top_lay = QVBoxLayout(ws_top); ws_top_lay.setContentsMargins(0, 0, 0, 0)
        ws_top_lay.addWidget(ws_h)

        ws_left_lay  = QVBoxLayout(ws_left);  ws_left_lay.setContentsMargins(4, 4, 4, 4);  ws_left_lay.setSpacing(2)
        ws_right_lay = QVBoxLayout(ws_right); ws_right_lay.setContentsMargins(4, 4, 4, 4); ws_right_lay.setSpacing(2)

        # ── LEFT: WS message log ───────────────────────────────────────────
        ws_left_lay.addWidget(_lbl(ws_left, "WebSocket Messages", font=_mono_lg, color=_ACC))
        ws_filter_row = QWidget(ws_left)
        ws_filter_layout = QHBoxLayout(ws_filter_row)
        ws_filter_layout.setContentsMargins(0, 0, 0, 0)
        ws_filter_layout.setSpacing(4)
        ws_filter_edit = _entry(ws_filter_row)
        ws_filter_edit.setPlaceholderText("Filter by origin or path…")
        ws_filter_layout.addWidget(ws_filter_edit, 1)
        ws_filter_payload_cb = QCheckBox("Payload", ws_filter_row)
        ws_filter_payload_cb.setToolTip(
            "Include the first 8 KiB of each UTF-8 payload in text filtering.")
        ws_filter_layout.addWidget(ws_filter_payload_cb)
        ws_dir_filter = QComboBox(ws_filter_row)
        ws_dir_filter.addItems(["All directions", "Incoming", "Outgoing"])
        ws_dir_filter.setFont(_mono_sm)
        ws_filter_layout.addWidget(ws_dir_filter)
        ws_op_filter = QComboBox(ws_filter_row)
        ws_op_filter.addItems(["All opcodes", "Text", "Binary"])
        ws_op_filter.setFont(_mono_sm)
        ws_filter_layout.addWidget(ws_op_filter)
        ws_filter_count = _lbl(ws_filter_row, "0 / 0", color=_DIM)
        ws_filter_layout.addWidget(ws_filter_count)
        ws_left_lay.addWidget(ws_filter_row)

        ws_cols = ["ts", "dir", "op", "origin", "path", "size"]
        ws_labels = ["Time", "Dir", "Op", "Origin", "Path", "Size"]
        ws_widths = {"ts": 58, "dir": 36, "op": 50, "origin": 130, "size": 56}
        ws_tree = QTreeWidget(ws_left)
        ws_tree.setColumnCount(len(ws_cols))
        ws_tree.setHeaderLabels(ws_labels)
        ws_tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        ws_tree.setRootIsDecorated(False)
        ws_tree.setFont(_mono)
        ws_hdr = ws_tree.header()
        for i, c in enumerate(ws_cols):
            if c == "path":
                ws_hdr.setSectionResizeMode(i, QHeaderView.ResizeMode.Stretch)
            else:
                ws_hdr.setSectionResizeMode(i, QHeaderView.ResizeMode.Interactive)
                if c in ws_widths:
                    ws_tree.setColumnWidth(i, ws_widths[c])
        ws_left_lay.addWidget(ws_tree, 1)

        ws_ctrl = QWidget(ws_left); ws_ctrl.setStyleSheet(f"background: {_PNL};")
        wsc = QHBoxLayout(ws_ctrl); wsc.setContentsMargins(8, 0, 8, 0); wsc.setSpacing(6)
        ws_left_lay.addWidget(ws_ctrl)
        ws_auto_scroll_cb = QCheckBox("Auto-scroll", ws_ctrl); ws_auto_scroll_cb.setChecked(True)
        ws_paused_cb = QCheckBox("Pause", ws_ctrl)
        wsc.addWidget(ws_auto_scroll_cb); wsc.addWidget(ws_paused_cb)
        _ws_row_data: dict[str, dict] = {}
        _ws_row_payload_search: dict[str, str] = {}
        _ws_row_counter = [0]
        _WS_MAX_ROWS = 800

        def _ws_clear():
            ws_tree.clear()
            _ws_row_data.clear()
            _ws_row_payload_search.clear()
            _ws_apply_filters()
        wsc.addWidget(_btn(ws_ctrl, "Clear", _ws_clear))
        wsc.addStretch(1)

        def _ws_apply_filters(scroll=False):
            query = ws_filter_edit.text().strip().casefold()
            search_payload = ws_filter_payload_cb.isChecked()
            direction_filter = ws_dir_filter.currentText()
            opcode_filter = ws_op_filter.currentText()
            visible = 0
            total = ws_tree.topLevelItemCount()
            last_visible = None
            for row in range(total):
                item = ws_tree.topLevelItem(row)
                iid = item.data(0, Qt.ItemDataRole.UserRole)
                data = _ws_row_data.get(iid, {})
                direction = data.get("direction")
                opcode = data.get("opcode")
                searchable = f"{data.get('origin', '')} {data.get('path', '')}".casefold()
                if search_payload:
                    searchable += " " + _ws_row_payload_search.get(iid, "")
                matches = (
                    (not query or query in searchable)
                    and (direction_filter == "All directions"
                         or (direction_filter == "Incoming" and direction == "in")
                         or (direction_filter == "Outgoing" and direction == "out"))
                    and (opcode_filter == "All opcodes"
                         or (opcode_filter == "Text" and opcode == 1)
                         or (opcode_filter == "Binary" and opcode == 2))
                )
                item.setHidden(not matches)
                if matches:
                    visible += 1
                    last_visible = item
            if ws_paused_cb.isChecked():
                queued = _s2l_state._gui_ws_log_queue.qsize()
                ws_filter_count.setText(
                    f"Paused (+{queued} queued)" if queued else
                    (f"{visible} / {total} · Paused" if total else "Paused"))
                ws_filter_count.setToolTip(
                    f"{visible} visible out of {total} captured; "
                    f"{queued} message(s) waiting in the queue.")
            elif total:
                ws_filter_count.setText(f"{visible} / {total}")
                ws_filter_count.setToolTip(
                    f"{visible} WebSocket message(s) visible out of {total} captured.")
            else:
                ws_filter_count.setText("Waiting for frames")
                ws_filter_count.setToolTip(
                    "The WebSocket connection can be open while idle. "
                    "Interact with the page to make it send or receive a frame.")
            if scroll and ws_auto_scroll_cb.isChecked() and last_visible is not None:
                ws_tree.scrollToItem(last_visible)

        ws_filter_edit.textChanged.connect(lambda *_: _ws_apply_filters())
        ws_filter_payload_cb.toggled.connect(lambda *_: _ws_apply_filters())
        ws_dir_filter.currentIndexChanged.connect(lambda *_: _ws_apply_filters())
        ws_op_filter.currentIndexChanged.connect(lambda *_: _ws_apply_filters())
        ws_paused_cb.toggled.connect(lambda *_: _ws_apply_filters())
        _ws_apply_filters()

        # ── RIGHT: WS hook editor ──────────────────────────────────────────
        ws_right_lay.addWidget(_lbl(ws_right, "WebSocket Hook Editor", font=_mono_lg, color=_ACC))
        ws_info_row = QWidget(ws_right)
        ws_info_layout = QHBoxLayout(ws_info_row)
        ws_info_layout.setContentsMargins(0, 0, 0, 0)
        ws_info_layout.setSpacing(4)
        ws_info_lbl = _lbl(ws_info_row, "← Select a WS message", color=_ACC)
        ws_info_lbl.setMinimumWidth(0)
        ws_info_lbl.setSizePolicy(
            QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        ws_info_layout.addWidget(ws_info_lbl, 1)
        ws_load_selected_btn = _btn(
            ws_info_row, "Set match from selected",
            lambda: _ws_load_selected(force=True),
            font=_mono_sm)
        ws_load_selected_btn.setObjectName("s2l_ws_load_selected")
        ws_load_selected_btn.setToolTip(
            "Set the selected message as the original match target, even while "
            "Freeze is on. The replacement editor also loads this message.")
        ws_info_layout.addWidget(ws_load_selected_btn)
        ws_right_lay.addWidget(ws_info_row)

        ws_row1 = QWidget(ws_right); wr1 = QHBoxLayout(ws_row1); wr1.setContentsMargins(0, 0, 0, 0); wr1.setSpacing(4)
        ws_row2 = QWidget(ws_right); wr2 = QHBoxLayout(ws_row2); wr2.setContentsMargins(0, 0, 0, 0); wr2.setSpacing(4)
        ws_row3 = QWidget(ws_right); wr3 = QHBoxLayout(ws_row3); wr3.setContentsMargins(0, 0, 0, 0); wr3.setSpacing(4)
        ws_right_lay.addWidget(ws_row1); ws_right_lay.addWidget(ws_row2); ws_right_lay.addWidget(ws_row3)

        wr1.addWidget(_lbl(ws_row1, "Name:", color=_ACC))
        ws_name_edit = _entry(ws_row1, width=14); wr1.addWidget(ws_name_edit)
        wr1.addWidget(_lbl(ws_row1, "Dir:", color=_ACC))
        ws_dir_combo = QComboBox(ws_row1); ws_dir_combo.setEditable(False)
        ws_dir_combo.addItems(["*", "in", "out"]); ws_dir_combo.setFont(_mono)
        ws_dir_combo.setMaximumWidth(70); wr1.addWidget(ws_dir_combo)
        wr1.addWidget(_lbl(ws_row1, "Op:", color=_ACC))
        ws_op_combo = QComboBox(ws_row1); ws_op_combo.setEditable(False)
        ws_op_combo.addItems(["*", "text", "bin"]); ws_op_combo.setFont(_mono)
        ws_op_combo.setMaximumWidth(80); wr1.addWidget(ws_op_combo)
        wr1.addStretch(1)

        wr2.addWidget(_lbl(ws_row2, "Pattern URL:", color=_ACC))
        ws_pat_edit = _entry(ws_row2); ws_pat_edit.setText(r".*")
        wr2.addWidget(ws_pat_edit, 1)

        wr3.addWidget(_lbl(ws_row3, "Origin URL:", color=_ACC))
        ws_origin_edit = _entry(ws_row3); ws_origin_edit.setText("*")
        wr3.addWidget(ws_origin_edit, 1)
        wr3.addWidget(_lbl(ws_row3, "  (target / * / host)", color=_DIM))

        # ── Match mode (internal — auto-determined) ───────────────────────
        ws_match_mode = ["any"]            # current mode (mutable holder)
        ws_match_payload_holder: list[bytes] = [b""]

        # ── Bottom rows (added FIRST so they sit at the bottom) ───────────
        ws_btn_row = QWidget(ws_right); ws_btn_row.setStyleSheet(f"background: {_PNL};")
        wsbr = QHBoxLayout(ws_btn_row); wsbr.setContentsMargins(0, 0, 0, 0); wsbr.setSpacing(4)
        ws_right_lay.addWidget(ws_btn_row)
        ws_save_status_lbl = _lbl(ws_right, "", color=_DIM)
        ws_right_lay.addWidget(ws_save_status_lbl)

        # ── When → Then layout (horizontal splitter) ─────────────────────
        ws_when_then = QSplitter(Qt.Orientation.Horizontal, ws_right)
        ws_when_then.setChildrenCollapsible(False)
        ws_when_frame  = QWidget(); ws_when_frame.setStyleSheet(f"background: {_PNL};")
        ws_then_frame  = QWidget(); ws_then_frame.setStyleSheet(f"background: {_PNL};")
        ws_when_then.addWidget(ws_when_frame)
        ws_when_then.addWidget(ws_then_frame)
        ws_when_then.setStretchFactor(0, 45)
        ws_when_then.setStretchFactor(1, 55)
        ws_when_then.setSizes([300, 380])
        ws_right_lay.addWidget(ws_when_then, 1)

        wwf = QVBoxLayout(ws_when_frame); wwf.setContentsMargins(4, 2, 4, 2); wwf.setSpacing(2)
        wtf = QVBoxLayout(ws_then_frame); wtf.setContentsMargins(4, 2, 4, 2); wtf.setSpacing(2)

        wwf.addWidget(_lbl(ws_when_frame, "Original payload to match (kept intact):",
                           font=_mono_sm_b, color=_RED))

        ws_when_top_row = QWidget(ws_when_frame); ws_when_top_row.setStyleSheet(f"background: {_PNL};")
        wwtr = QHBoxLayout(ws_when_top_row); wwtr.setContentsMargins(0, 0, 0, 0); wwtr.setSpacing(4)
        wwf.addWidget(ws_when_top_row)
        wwtr.addWidget(_lbl(ws_when_frame,
                             "(read-only — saved as the original match target)",
                             font=_mono_xs, color=_DIM), 1)
        ws_freeze_cb = QCheckBox("Freeze match", ws_when_top_row); ws_freeze_cb.setFont(_mono_xs)
        ws_freeze_cb.setToolTip(
            "Lock the original payload in the When panel as the match target. "
            "Selecting another message will still load it into the replacement "
            "editor, so you can edit what should replace the original.")
        ws_freeze_lbl = _lbl(ws_when_top_row, "", font=_mono_sm_b, color=_ACC)
        ws_match_mode_combo = QComboBox(ws_when_top_row)
        ws_match_mode_combo.addItems([
            "Exact payload", "Any payload", "Regex payload", "Contains payload"])
        ws_match_mode_combo.setFont(_mono_xs)
        ws_match_mode_combo.setCurrentText("Any payload")
        ws_match_mode_combo.setToolTip(
            "Choose whether a hook matches any payload, exact bytes, a regex, or contained bytes.")
        wwtr.addWidget(ws_freeze_lbl)
        wwtr.addWidget(ws_match_mode_combo)
        wwtr.addWidget(ws_freeze_cb)

        def _on_freeze_toggle(_state):
            if ws_freeze_cb.isChecked():
                ws_freeze_lbl.setText("[FROZEN]")
                ws_freeze_lbl.setStyleSheet(f"color: {_ACC}; background: transparent;")
            else:
                ws_freeze_lbl.setText("")
        ws_freeze_cb.stateChanged.connect(_on_freeze_toggle)

        ws_when_tabs = QTabWidget(ws_when_frame)
        ws_when_tabs.setFont(_mono_sm)
        ws_when_text = _text_box(ws_when_tabs, wrap=True, readonly=True)
        ws_when_text.setStyleSheet(f"background: {_PNL}; color: {_YLW};")
        ws_when_json = _text_box(ws_when_tabs, wrap=False, readonly=True)
        ws_when_json.setStyleSheet(f"background: {_PNL}; color: {_FG};")
        ws_when_hex = _text_box(ws_when_tabs, wrap=False, readonly=True)
        ws_when_hex.setObjectName("s2l_ws_match_hex")
        ws_when_hex.setStyleSheet(f"background: {_PNL}; color: {_GRN};")
        ws_when_tabs.addTab(ws_when_text, "Text")
        ws_when_tabs.addTab(ws_when_json, "JSON")
        ws_when_tabs.addTab(ws_when_hex, "Hex")
        wwf.addWidget(ws_when_tabs, 1)

        # ── RIGHT: THEN panel (synchronized editable hex and text) ────────
        wtf.addWidget(_lbl(ws_then_frame,
                            "Replacement payload (saved separately from the original):",
                            font=_mono_sm_b, color=_GRN))

        ws_then_tabs = QTabWidget(ws_then_frame)
        ws_then_tabs.setFont(_mono_sm)
        ws_hex_box = _text_box(ws_then_tabs, wrap=False)
        ws_hex_box.setObjectName("s2l_ws_replacement_hex")
        ws_hex_box.setPlaceholderText(
            "Enter bytes such as: 48 65 6c 6c 6f\n"
            "Offsets are optional; bytes can be added or removed freely.")
        ws_hex_box.setToolTip(
            "Space-separated bytes. No offsets or length fields are required.")
        ws_hex_box.setStyleSheet(f"background: {_PNL}; color: {_GRN};")
        _ws_sp = QSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Ignored)
        ws_hex_box.setSizePolicy(_ws_sp)
        ws_txt_box = _text_box(ws_then_tabs, wrap=True)
        ws_txt_box.setObjectName("s2l_ws_replacement_text")
        ws_txt_box.setStyleSheet(f"background: {_PNL_ALT}; color: {_FG};")
        ws_txt_box.setSizePolicy(_ws_sp)
        ws_encoding_row = QWidget(ws_then_frame)
        ws_encoding_layout = QHBoxLayout(ws_encoding_row)
        ws_encoding_layout.setContentsMargins(0, 0, 0, 0)
        ws_encoding_layout.setSpacing(4)
        ws_encoding_layout.addWidget(_lbl(
            ws_encoding_row, "Text encoding:", color=_ACC))
        ws_encoding_combo = QComboBox(ws_encoding_row)
        ws_encoding_combo.setObjectName("s2l_ws_encoding")
        ws_encoding_combo.setEditable(True)
        ws_encoding_combo.addItems([
            "Auto", "UTF-8", "UTF-8-SIG", "UTF-16", "UTF-16-LE",
            "UTF-16-BE", "UTF-32", "UTF-32-LE", "UTF-32-BE",
            "Windows-1252", "ISO-8859-1", "Shift-JIS", "GB18030",
            "EUC-KR", "Big5",
        ])
        ws_encoding_combo.setCurrentText("Auto")
        ws_encoding_combo.setToolTip(
            "Auto detects BOMs and common Unicode byte patterns; choose or type "
            "any encoding supported by Python's codecs for other payloads.")
        ws_encoding_layout.addWidget(ws_encoding_combo, 1)
        wtf.addWidget(ws_encoding_row)
        ws_then_tabs.addTab(ws_hex_box, "Hex (raw bytes)")
        ws_then_tabs.addTab(ws_txt_box, "Text")
        ws_then_tabs.setToolTip(
            "Edit either representation. Text uses the selected encoding; "
            "use Hex for arbitrary binary payloads.")
        wtf.addWidget(ws_then_tabs, 1)

        # ── Hex / text helpers + bidirectional live sync ─────────────────
        def _bytes_to_hexdump(data: bytes) -> str:
            """Format editable bytes without offsets or length fields."""
            if not data:
                return ""
            return "\n".join(
                " ".join(f"{b:02x}" for b in data[off:off + 16])
                for off in range(0, len(data), 16)
            )

        def _hexdump_to_bytes(text: str):
            """Parse raw hex bytes or legacy offset-prefixed dumps."""
            out = bytearray()
            for line in text.splitlines():
                stripped = re.sub(
                    r"^[0-9a-fA-F]{8}\s{2,}", "", line).strip()
                if not stripped:
                    continue
                for tok in stripped.split():
                    if not re.fullmatch(r"[0-9a-fA-F]+", tok):
                        return None
                    if len(tok) % 2 != 0:
                        return None
                    out.extend(bytes.fromhex(tok))
            return bytes(out)

        _ws_syncing_editors = [False]
        _ws_text_sync_error = [False]

        def _ws_auto_encoding(data: bytes) -> str:
            """Detect common Unicode encodings; arbitrary legacy encodings are ambiguous."""
            for marker, encoding in (
                (codecs.BOM_UTF32_LE, "utf-32"),
                (codecs.BOM_UTF32_BE, "utf-32"),
                (codecs.BOM_UTF8, "utf-8-sig"),
                (codecs.BOM_UTF16_LE, "utf-16"),
                (codecs.BOM_UTF16_BE, "utf-16"),
            ):
                if data.startswith(marker):
                    return encoding
            sample = data[:256]
            if len(sample) >= 4:
                even_nuls = sum(sample[i] == 0 for i in range(0, len(sample), 2))
                odd_nuls = sum(sample[i] == 0 for i in range(1, len(sample), 2))
                pairs = len(sample) // 2
                if even_nuls > pairs * 0.3 and odd_nuls == 0:
                    return "utf-16-be"
                if odd_nuls > pairs * 0.3 and even_nuls == 0:
                    return "utf-16-le"
            try:
                data.decode("utf-8", errors="strict")
                return "utf-8"
            except UnicodeDecodeError:
                return "cp1252"

        def _ws_selected_encoding(data: bytes) -> tuple[str, str]:
            requested = ws_encoding_combo.currentText().strip()
            if not requested or requested.casefold() == "auto":
                encoding = _ws_auto_encoding(data)
            else:
                try:
                    encoding = codecs.lookup(requested).name
                except LookupError as exc:
                    raise ValueError(
                        f"Unknown encoding '{requested}'. Choose or type a Python codec name."
                    ) from exc
            return requested or "Auto", encoding

        def _ws_sync_text_from_hex(*_):
            """Decode valid replacement bytes using the selected codec."""
            if _ws_syncing_editors[0]:
                return
            parsed = _hexdump_to_bytes(ws_hex_box.toPlainText())
            if parsed is None:
                _ws_text_sync_error[0] = True
                ws_save_status_lbl.setText("Hex invalid: odd digit count or non-hex character.")
                ws_save_status_lbl.setStyleSheet(f"color: {_RED}; background: transparent;")
                return
            try:
                requested, encoding = _ws_selected_encoding(parsed)
            except ValueError as exc:
                _ws_text_sync_error[0] = True
                ws_save_status_lbl.setText(str(exc))
                ws_save_status_lbl.setStyleSheet(f"color: {_RED}; background: transparent;")
                return
            lossy = False
            try:
                decoded = parsed.decode(encoding, errors="strict")
            except UnicodeError:
                try:
                    decoded = parsed.decode(encoding, errors="replace")
                except UnicodeError as exc:
                    _ws_text_sync_error[0] = True
                    ws_save_status_lbl.setText(
                        f"Cannot decode with {encoding}: {exc}")
                    ws_save_status_lbl.setStyleSheet(
                        f"color: {_RED}; background: transparent;")
                    return
                lossy = True
            _ws_syncing_editors[0] = True
            try:
                ws_txt_box.setPlainText(decoded)
            finally:
                _ws_syncing_editors[0] = False
            _ws_text_sync_error[0] = False
            detected = f" · detected {encoding}" if requested.casefold() == "auto" else f" · {encoding}"
            message = f"Replacement valid: {len(parsed)} byte(s){detected}."
            if lossy:
                message += " Decode used replacement characters; select another encoding."
            ws_save_status_lbl.setText(message)
            ws_save_status_lbl.setStyleSheet(
                f"color: {_YLW if lossy else _DIM}; background: transparent;")

        def _ws_sync_hex_from_text(*_):
            """Encode text edits with the chosen codec and refresh the hex editor."""
            if _ws_syncing_editors[0]:
                return
            try:
                current_data = _hexdump_to_bytes(ws_hex_box.toPlainText()) or b""
                requested, encoding = _ws_selected_encoding(
                    current_data)
                data = ws_txt_box.toPlainText().encode(encoding, errors="strict")
            except (LookupError, UnicodeError, ValueError) as exc:
                _ws_text_sync_error[0] = True
                ws_save_status_lbl.setText(f"Text cannot be encoded: {exc}")
                ws_save_status_lbl.setStyleSheet(f"color: {_RED}; background: transparent;")
                return
            _ws_syncing_editors[0] = True
            try:
                ws_hex_box.setPlainText(_bytes_to_hexdump(data))
            finally:
                _ws_syncing_editors[0] = False
            _ws_text_sync_error[0] = False
            label = encoding if requested.casefold() == "auto" else requested
            ws_save_status_lbl.setText(
                f"Text encoded as {label}: {len(data)} byte(s).")
            ws_save_status_lbl.setStyleSheet(f"color: {_DIM}; background: transparent;")

        ws_hex_box.textChanged.connect(_ws_sync_text_from_hex)
        ws_txt_box.textChanged.connect(_ws_sync_hex_from_text)
        ws_encoding_combo.currentTextChanged.connect(_ws_sync_text_from_hex)

        # ── WHEN panel updater ───────────────────────────────────────────
        def _ws_update_when_panel(*_):
            mode = (ws_match_mode[0] or "any").strip().lower() or "any"
            mp = ws_match_payload_holder[0]
            if mode == "any" or not mp:
                ws_when_text.setPlainText(
                    "Any payload matching the URL, direction and opcode filters "
                    "will trigger this hook.\n\n"
                    "Freeze locks only the original match target. Selecting another "
                    "message still loads it into the replacement editor.")
                ws_when_json.setPlainText("No exact payload selected.")
                ws_when_hex.setPlainText("")
                ws_when_text.setStyleSheet(f"background: {_PNL}; color: {_DIM};")
            else:
                decoded = mp.decode("utf-8", errors="replace")
                display = "".join(
                    c if (c.isprintable() or c in "\n\r\t") else "·"
                    for c in decoded
                )
                if mode == "regex":
                    ws_when_text.setPlainText(
                        f"[payload regex]\n{display}\n\n"
                        "Only payloads matching this expression trigger the hook.")
                elif mode == "contains":
                    ws_when_text.setPlainText(
                        f"[payload contains]\n{display}\n\n"
                        "Only payloads containing these bytes trigger the hook.")
                else:
                    ws_when_text.setPlainText(display)
                try:
                    pretty_json = json.dumps(
                        json.loads(decoded), indent=2, ensure_ascii=False)
                except (json.JSONDecodeError, RecursionError):
                    pretty_json = "This payload is not valid JSON."
                ws_when_json.setPlainText(pretty_json)
                ws_when_hex.setPlainText(_bytes_to_hexdump(mp))
                ws_when_text.setStyleSheet(f"background: {_PNL}; color: {_YLW};")

        def _ws_match_mode_changed(text):
            ws_match_mode[0] = {
                "Exact payload": "exact",
                "Any payload": "any",
                "Regex payload": "regex",
                "Contains payload": "contains",
            }.get(text, "any")
            _ws_update_when_panel()

        ws_match_mode_combo.currentTextChanged.connect(_ws_match_mode_changed)
        _ws_update_when_panel()

        def _ws_load_selected(force=False):
            items = ws_tree.selectedItems()
            if not items:
                ws_save_status_lbl.setText("Select a WebSocket message first.")
                ws_save_status_lbl.setStyleSheet(
                    f"color: {_YLW}; background: transparent;")
                return
            item = items[0]
            iid = item.data(0, Qt.ItemDataRole.UserRole)
            data = _ws_row_data.get(iid)
            if not data:
                return
            payload = data.get("payload") or b""
            _full_len = len(payload)
            # Cap the payload used by the editor / matcher so a multi-MB WS
            # message can't freeze the UI or inflate the WHEN panel. The full
            # message is still captured to disk when CAPTURE_WS is on.
            if _full_len > _s2l_state._EDITOR_MAX_BYTES:
                payload = payload[:_s2l_state._EDITOR_MAX_BYTES]
            _dir_raw = data.get("direction", "?")
            _dir_disp = "[->] IN" if _dir_raw == "in" else ("[<-] OUT" if _dir_raw == "out" else _dir_raw.upper())
            _op_disp = (data.get("op_name") or "?").upper()
            _trunc_note = (f"  · editor-capped @ {_s2l_state._fmt_size(_s2l_state._EDITOR_MAX_BYTES)}"
                           if _full_len > _s2l_state._EDITOR_MAX_BYTES else "")
            ws_info_text = (
                f"{_dir_disp}  {_op_disp}  {data['ws_url']}  "
                f"[{_s2l_state._fmt_size(_full_len)}]{_trunc_note}")
            ws_info_lbl.setText(ws_info_text)
            ws_info_lbl.setToolTip(ws_info_text)
            ws_hex_box.setPlainText(_bytes_to_hexdump(payload))
            _ws_sync_text_from_hex()
            ws_dir_combo.setCurrentText(_dir_raw if _dir_raw in ("in", "out") else "*")
            ws_op_combo.setCurrentText({1: "text", 2: "bin"}.get(data.get("opcode", 0), "*"))
            try:
                ws_pat_edit.setText(re.escape(urlparse(data["ws_url"]).path or "/"))
            except Exception:
                ws_pat_edit.setText(r".*")
            ws_origin_edit.setText(data.get("origin", "") or "*")
            if not ws_name_edit.text().strip():
                ws_name_edit.setText(f"ws_{_dir_raw}_{_op_disp.lower()}")
            if not ws_freeze_cb.isChecked() or force:
                ws_match_payload_holder[0] = bytes(payload)
                ws_match_mode[0] = "exact"
                ws_match_mode_combo.setCurrentText("Exact payload")
            _ws_update_when_panel()
            if ws_freeze_cb.isChecked() and not force:
                ws_save_status_lbl.setText(
                    "Replacement loaded from selected message; frozen match target unchanged.")
                ws_save_status_lbl.setStyleSheet(
                    f"color: {_ACC}; background: transparent;")

        def _ws_on_select(*_):
            _ws_load_selected()

        ws_tree.itemSelectionChanged.connect(_ws_on_select)

        # ── Disk persistence + Save / Test buttons ───────────────────────
        _ws_hook_save_to_disk, _ws_hook_delete_from_disk, _ws_hook_load_all = \
            _s2l_state._make_hook_store("MyWSHooks", extra_binary_fields=("match_payload",))

        def _ws_save_hook():
            name = ws_name_edit.text().strip()
            if not name:
                QMessageBox.warning(win, "S2L", "Hook name is required.")
                return
            if (ws_then_tabs.currentWidget() is ws_txt_box
                    and _ws_text_sync_error[0]):
                QMessageBox.warning(win, "S2L",
                    "The text editor is not synchronized. Fix the encoding or "
                    "switch to Hex and correct the payload before saving.")
                return
            raw_hex = ws_hex_box.toPlainText()
            body_bytes = _hexdump_to_bytes(raw_hex)
            if body_bytes is None:
                QMessageBox.critical(win, "S2L",
                    "Invalid hex — fix the highlighted error before saving.")
                return
            if not body_bytes:
                QMessageBox.warning(win, "S2L",
                    "Hook payload is empty — nothing to inject.")
                return
            pat_str = ws_pat_edit.text().strip() or r".*"
            try:
                re.compile(pat_str)
            except re.error as e:
                QMessageBox.critical(win, "S2L", f"Invalid pattern: {e}")
                return
            op_map = {"text": 1, "bin": 2}
            opcode = op_map.get(ws_op_combo.currentText(), 0)
            frozen = ws_freeze_cb.isChecked()
            match_mode = ws_match_mode[0]
            match_payload = ws_match_payload_holder[0] if match_mode != "any" else b""
            if match_mode != "any" and not match_payload:
                QMessageBox.warning(win, "S2L",
                    "Select a captured message for a payload match, "
                    "or choose Any payload.")
                return
            if match_mode == "regex":
                try:
                    re.compile(match_payload.decode("utf-8", "ignore"), re.DOTALL)
                except re.error as e:
                    QMessageBox.critical(win, "S2L",
                        f"Invalid payload regex: {e}")
                    return
            # PERSISTENCE FIX: the WHEN panel's "Freeze" state is now part of
            # the hook itself. Previously `frozen` lived only in the checkbox,
            # so after a restart editing a saved hook left Freeze unchecked —
            # and the very next message selection silently overwrote the
            # frozen match_payload, defeating the whole feature. Persisting
            # `frozen` alongside match_payload keeps the WHEN content stable
            # across restarts: the freeze guard in _ws_on_select reads this
            # flag (restored into the checkbox on edit) and refuses to touch
            # the holder while it's set.
            hook = {
                "name":          name,
                "direction":     ws_dir_combo.currentText(),
                "opcode":        opcode,
                "pattern":       pat_str,
                "origin_url":    ws_origin_edit.text().strip() or "*",
                "body_bytes":    body_bytes,
                "enabled":       True,
                "match_mode":    match_mode,
                "match_payload": match_payload,
                "frozen":        frozen,
            }
            with _s2l_state._gui_ws_hooks_lock:
                existing = next((h for h in _s2l_state._gui_ws_hooks if h["name"] == name), None)
                if existing is not None:
                    if QMessageBox.question(win, "S2L",
                            f"A WS hook named '{name}' already exists.\nReplace it?"
                        ) != QMessageBox.StandardButton.Yes:
                        return
                    _s2l_state._gui_ws_hooks[:] = [h for h in _s2l_state._gui_ws_hooks if h["name"] != name]
                _s2l_state._gui_ws_hooks.append(hook)
            if not _ws_hook_save_to_disk(hook):
                ws_save_status_lbl.setText(
                    f"'{name}' is active for this session, but disk save failed. "
                    "Check the terminal log.")
                ws_save_status_lbl.setStyleSheet(
                    f"color: {_RED}; background: transparent;")
                _render_ws_hook_rows()
                return
            _render_ws_hook_rows()
            _trigger_desc = {
                "exact": "exact payload",
                "any": "any payload",
                "regex": "payload regex",
                "contains": "payload contains",
            }.get(match_mode, "any payload")
            ws_save_status_lbl.setText(
                f"Saved '{name}'  [dir={hook['direction']} op={ws_op_combo.currentText()} "
                f"trigger={_trigger_desc}]  {hook['pattern']}")
            ws_save_status_lbl.setStyleSheet(f"color: {_GRN}; background: transparent;")
            _s2l_state.log(f"GUI WS hook saved: '{name}' [dir={hook['direction']} "
                f"op={ws_op_combo.currentText()} trigger={_trigger_desc}] {hook['pattern']}", "HOOK")

        def _ws_test_hook():
            pat_str = ws_pat_edit.text().strip()
            test_path, ok = QInputDialog.getText(win, "Test WS Hook",
                "Enter a WS path to test (e.g. /ws/chat):")
            if not ok or test_path is None:
                return
            try:
                matched = bool(re.search(pat_str, test_path, re.IGNORECASE))
            except re.error as e:
                QMessageBox.critical(win, "S2L", f"Invalid pattern: {e}")
                return
            ws_save_status_lbl.setText(
                f"{'MATCH' if matched else 'NO MATCH'}: pattern against '{test_path}'")
            ws_save_status_lbl.setStyleSheet(
                f"color: {_GRN if matched else _RED}; background: transparent;")

        _sb2 = _btn(ws_btn_row, "Save WS Hook", _ws_save_hook, font=_mono_b)
        wsbr.addWidget(_sb2, 1)
        wsbr.addWidget(_btn(ws_btn_row, "Test Pattern", _ws_test_hook, font=_mono_sm))

        # ── BOTTOM: Active WS Hooks ───────────────────────────────────────
        ws_bot_lay = QVBoxLayout(ws_bot); ws_bot_lay.setContentsMargins(0, 4, 0, 0); ws_bot_lay.setSpacing(2)
        ws_bot_lay.addWidget(_lbl(ws_bot, "Active WebSocket Hooks", font=_mono_lg, color=_ACC))

        ws_hooks_scroll = QScrollArea(ws_bot)
        ws_hooks_scroll.setWidgetResizable(True)
        ws_hooks_inner = QWidget(); ws_hooks_inner.setStyleSheet(f"background: {_PNL};")
        ws_hooks_grid = QGridLayout(ws_hooks_inner)
        ws_hooks_grid.setContentsMargins(8, 4, 8, 4); ws_hooks_grid.setSpacing(4)
        ws_hooks_scroll.setWidget(ws_hooks_inner)
        ws_bot_lay.addWidget(ws_hooks_scroll, 1)

        _WS_HOOK_COLS   = ("Name", "Dir", "Op", "Origin", "Pattern", "On", "", "")
        _WS_HOOK_WIDTHS = (12, 5, 5, 14, 22, 4, 4, 4)

        def _render_ws_hook_rows():
            while ws_hooks_grid.count():
                it = ws_hooks_grid.takeAt(0)
                w = it.widget()
                if w is not None:
                    w.deleteLater()
            for col_idx, (col_name, col_w) in enumerate(zip(_WS_HOOK_COLS, _WS_HOOK_WIDTHS)):
                lbl = _lbl(ws_hooks_inner, col_name, font=_mono_sm_b, color=_ACC)
                lbl.setMinimumWidth(col_w * 8)
                ws_hooks_grid.addWidget(lbl, 0, col_idx, Qt.AlignmentFlag.AlignLeft)
            with _s2l_state._gui_ws_hooks_lock:
                hooks_copy = list(_s2l_state._gui_ws_hooks)
            if not hooks_copy:
                ws_hooks_grid.addWidget(_lbl(ws_hooks_inner, "No WS hooks saved yet.", color=_DIM),
                                        1, 0, 1, 8, Qt.AlignmentFlag.AlignLeft)
                return
            op_name_map = {1: "TEXT", 2: "BIN", 0: "*"}
            for row_idx, h in enumerate(hooks_copy, start=1):
                row_fg = _FG if h["enabled"] else _DIM
                ws_hooks_grid.addWidget(_lbl(ws_hooks_inner, h["name"], color=row_fg),
                                         row_idx, 0, Qt.AlignmentFlag.AlignLeft)
                ws_hooks_grid.addWidget(_lbl(ws_hooks_inner,
                                             h.get("direction", "*") or "*",
                                             color=_YLW if h["enabled"] else _DIM),
                                         row_idx, 1, Qt.AlignmentFlag.AlignLeft)
                ws_hooks_grid.addWidget(_lbl(ws_hooks_inner,
                                             op_name_map.get(h.get("opcode", 0), "*"),
                                             color=_GRN if h["enabled"] else _DIM),
                                         row_idx, 2, Qt.AlignmentFlag.AlignLeft)
                _origin_disp = h.get("origin_url", "*") or "*"
                if len(_origin_disp) > 14:
                    _origin_disp = _origin_disp[:13] + "…"
                ws_hooks_grid.addWidget(_lbl(ws_hooks_inner, _origin_disp,
                                             color=_CYN if h["enabled"] else _DIM),
                                         row_idx, 3, Qt.AlignmentFlag.AlignLeft)
                _pat_disp = h["pattern"][:22] + ("…" if len(h["pattern"]) > 22 else "")
                ws_hooks_grid.addWidget(_lbl(ws_hooks_inner, _pat_disp, color=_DIM),
                                         row_idx, 4, Qt.AlignmentFlag.AlignLeft)

                cb = QCheckBox(ws_hooks_inner)
                cb.setChecked(h["enabled"])

                def _make_ws_toggle(hook_ref, checkbox):
                    def _toggle(_checked):
                        with _s2l_state._gui_ws_hooks_lock:
                            hook_ref["enabled"] = checkbox.isChecked()
                        _ws_hook_save_to_disk(hook_ref)
                        _render_ws_hook_rows()
                    return _toggle
                cb.stateChanged.connect(_make_ws_toggle(h, cb))
                ws_hooks_grid.addWidget(cb, row_idx, 5, Qt.AlignmentFlag.AlignCenter)

                def _make_ws_edit(hook_ref):
                    def _edit():
                        ws_name_edit.setText(hook_ref["name"])
                        ws_dir_combo.setCurrentText(hook_ref.get("direction", "*") or "*")
                        op_val = hook_ref.get("opcode", 0)
                        ws_op_combo.setCurrentText({1: "text", 2: "bin"}.get(op_val, "*"))
                        ws_pat_edit.setText(hook_ref["pattern"])
                        ws_origin_edit.setText(hook_ref.get("origin_url", "*") or "*")
                        ws_hex_box.setPlainText(_bytes_to_hexdump(hook_ref["body_bytes"]))
                        _ws_sync_text_from_hex()
                        # Restore instanced-match fields: set the payload holder
                        # BEFORE the mode, because _ws_update_when_panel reads
                        # the holder to render the WHEN panel.
                        ws_match_payload_holder[0] = hook_ref.get("match_payload", b"") or b""
                        ws_match_mode[0] = hook_ref.get("match_mode", "any") or "any"
                        ws_match_mode_combo.setCurrentText(
                            {
                                "exact": "Exact payload",
                                "any": "Any payload",
                                "regex": "Regex payload",
                                "contains": "Contains payload",
                            }.get(ws_match_mode[0], "Any payload"))
                        # Restore the Freeze checkbox so the WHEN content stays
                        # pinned after restart. setChecked fires stateChanged,
                        # which updates the [FROZEN] label via _on_freeze_toggle.
                        ws_freeze_cb.setChecked(bool(hook_ref.get("frozen", False)))
                        _ws_update_when_panel()
                        ws_save_status_lbl.setText(f"Editing '{hook_ref['name']}'")
                        ws_save_status_lbl.setStyleSheet(f"color: {_YLW}; background: transparent;")
                    return _edit
                _ws_ed_btn = _btn(ws_hooks_inner, "Ed", _make_ws_edit(h), font=_mono_xs)
                _ws_ed_btn.setStyleSheet(f"background: {_GRN_BG}; color: {_GRN}; border: 1px solid #2a4a35;")
                ws_hooks_grid.addWidget(_ws_ed_btn,
                                         row_idx, 6, Qt.AlignmentFlag.AlignCenter)

                def _make_ws_del(hook_name):
                    def _del():
                        with _s2l_state._gui_ws_hooks_lock:
                            _s2l_state._gui_ws_hooks[:] = [x for x in _s2l_state._gui_ws_hooks if x["name"] != hook_name]
                        _ws_hook_delete_from_disk(hook_name)
                        _render_ws_hook_rows()
                        ws_save_status_lbl.setText(f"Deleted '{hook_name}'.")
                        ws_save_status_lbl.setStyleSheet(f"color: {_YLW}; background: transparent;")
                    return _del
                _del_btn = _btn(ws_hooks_inner, "X", _make_ws_del(h["name"]), font=_mono_xs)
                _del_btn.setStyleSheet(f"background: {_RED_BG}; color: {_RED}; border: 1px solid #5a2a30;")
                ws_hooks_grid.addWidget(_del_btn, row_idx, 7, Qt.AlignmentFlag.AlignCenter)

        _render_ws_hook_rows()

        def _load_ws_hooks_from_disk() -> None:
            """Load previously saved WS hooks from site_data/MyWSHooks/{host}/."""
            for hook in _ws_hook_load_all({"name": "?", "direction": "*",
                                            "opcode": 0, "pattern": ".*",
                                            "origin_url": "*", "enabled": True,
                                            "match_mode": "any",
                                            "match_payload": b"",
                                            "frozen": False}):
                hook.setdefault("match_mode", "any")
                hook.setdefault("match_payload", b"")
                hook.setdefault("frozen", False)
                with _s2l_state._gui_ws_hooks_lock:
                    if hook["name"] not in [h["name"] for h in _s2l_state._gui_ws_hooks]:
                        _s2l_state._gui_ws_hooks.append(hook)
                        _fz = "· FROZEN" if hook.get("frozen") else "match=" + hook.get('match_mode', 'any')
                        _s2l_state.log(f"Loaded WS hook '{hook['name']}' ({_fz}) from disk", "HOOK")

        QTimer.singleShot(100, lambda: (_load_ws_hooks_from_disk(), _render_ws_hook_rows()))

        # ── WS log queue poller ───────────────────────────────────────────
        def _ws_poll():
            # Wrapped in try/except so a slot exception is logged, not
            # swallowed — same protection as the main _poll() loop.
            try:
                if ws_paused_cb.isChecked():
                    _ws_apply_filters()
                else:
                    count = 0
                    received = 0
                    while count < 60:
                        try:
                            entry = _s2l_state._gui_ws_log_queue.get_nowait()
                        except queue.Empty:
                            break
                        sz = entry["size"]
                        if sz >= 1024 * 1024:
                            sz_s = f"{sz / 1024 / 1024:.1f}MB"
                        elif sz >= 1024:
                            sz_s = f"{sz // 1024}KB"
                        elif sz > 0:
                            sz_s = f"{sz}B"
                        else:
                            sz_s = "—"
                        _dir = entry["direction"]
                        dir_disp = "[->] IN" if _dir == "in" else ("[<-] OUT" if _dir == "out" else _dir)
                        _op_upper = (entry.get("op_name") or "").upper()
                        color = QColor(_GRN) if _dir == "in" else QColor(_CYN)
                        item = QTreeWidgetItem([entry["ts"], dir_disp, _op_upper,
                                                 entry["origin"], entry["path"], sz_s])
                        for c in range(item.columnCount()):
                            item.setForeground(c, color)
                        _ws_row_counter[0] += 1
                        iid = str(_ws_row_counter[0])
                        item.setData(0, Qt.ItemDataRole.UserRole, iid)
                        ws_tree.addTopLevelItem(item)
                        _ws_row_data[iid] = entry
                        _ws_row_payload_search[iid] = (
                            (entry.get("payload") or b"")[:8192]
                            .decode("utf-8", "replace").casefold())
                        if ws_tree.topLevelItemCount() > _WS_MAX_ROWS:
                            removed = ws_tree.takeTopLevelItem(0)
                            removed_iid = removed.data(0, Qt.ItemDataRole.UserRole)
                            _ws_row_data.pop(removed_iid, None)
                            _ws_row_payload_search.pop(removed_iid, None)
                        count += 1
                        received += 1
                    if received:
                        _ws_apply_filters(scroll=True)
            except Exception as _e:
                import traceback as _tb
                _s2l_state.log(f"_ws_poll() raised {_e.__class__.__name__}: {_e}\n"
                    + "".join(_tb.format_exception(type(_e), _e, _e.__traceback__)),
                    "ERROR")
        ws_poll_timer = QTimer(ws_bot)
        ws_poll_timer.timeout.connect(_ws_poll)
        ws_poll_timer.start(150)

        # ── Main polling loop for the HTTP traffic log ────────────────────
        def _poll():
            # Wrap the whole body in try/except so a slot exception is LOGGED,
            # not swallowed. Without this, a single KeyError on a malformed
            # queue entry would silently break the timer on every tick and
            # the Traffic Log would stay forever empty with no clue why.
            try:
                if not paused_cb.isChecked():
                    count = 0
                    while count < 60:
                        try:
                            entry = _s2l_state._gui_log_queue.get_nowait()
                        except queue.Empty:
                            break
                        method = entry["method"]
                        status = entry["status"]
                        sz = entry["size"]
                        if sz >= 1024 * 1024:
                            sz_s = f"{sz / 1024 / 1024:.1f}MB"
                        elif sz >= 1024:
                            sz_s = f"{sz // 1024}KB"
                        elif sz > 0:
                            sz_s = f"{sz}B"
                        else:
                            sz_s = "—"
                        if entry.get("hooked"):
                            color = QColor(_PURP)
                        elif status >= 400:
                            color = QColor(_RED)
                        elif any(x in entry.get("ct", "") for x in ("json", "xml", "event-stream")):
                            color = QColor(_BLUE)
                        elif method == "GET":
                            color = QColor(_GRN)
                        elif method == "POST":
                            color = QColor(_YLW)
                        elif method in ("PUT", "PATCH"):
                            color = QColor(_YLW)
                        elif method == "DELETE":
                            color = QColor(_RED)
                        else:
                            color = QColor(_FG)
                        _origin_val = entry.get("origin", "") or ""
                        _web_type_val = entry.get("web_type", "") or "None"
                        item = QTreeWidgetItem([
                            entry["ts"], method, _origin_val, entry["path"],
                            str(status), _web_type_val, entry["ct"], sz_s,
                        ])
                        for c in range(item.columnCount()):
                            item.setForeground(c, color)
                        _row_counter[0] += 1
                        iid = str(_row_counter[0])
                        item.setData(0, Qt.ItemDataRole.UserRole, iid)
                        _row_data[iid] = entry
                        tree.addTopLevelItem(item)
                        if tree.topLevelItemCount() > _MAX_ROWS:
                            tree.takeTopLevelItem(0)
                        if auto_scroll_cb.isChecked():
                            tree.scrollToItem(item)
                        count += 1
                # NOTE: a periodic "GUI poll alive" diagnostic used to be emitted
                # here every ~50 ticks (~6 s). Removed because it polluted the
                # terminal log without adding actionable info — if the poller
                # dies, the Traffic Log stops updating, which is itself the
                # signal. The try/except below already logs the full traceback
                # if a slot raises, so we don't lose any debuggability.
            except Exception as _e:
                # Log the FULL traceback so the root cause is obvious. This is
                # the line that turns "GUI silently empty" into "here's why".
                import traceback as _tb
                _s2l_state.log(f"_poll() raised {_e.__class__.__name__}: {_e}\n"
                    + "".join(_tb.format_exception(type(_e), _e, _e.__traceback__)),
                    "ERROR")
        poll_timer = QTimer(win)
        poll_timer.setTimerType(Qt.TimerType.PreciseTimer)
        poll_timer.timeout.connect(_poll)
        poll_timer.start(120)
        _s2l_state.log("PyQt6 GUI ready — Traffic Log poller started "
            f"(HOOK_GUI={_s2l_state.HOOK_GUI}, interval=120ms)", "INFO")

        win.show()
        app.exec()   # blocks — caller must be on main thread

    _gui_main()
