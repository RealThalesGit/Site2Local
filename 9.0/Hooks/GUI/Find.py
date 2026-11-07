"""Site2Local: find-all-with-highlight helper for the hook GUI editors."""
from __future__ import annotations


def _find_all(edit, query: str, highlight_hex: str = "#ccae5a"):
    from PyQt6.QtGui import QColor, QTextCharFormat, QTextCursor
    from PyQt6.QtWidgets import QTextEdit

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
        fmt.setBackground(QColor(highlight_hex))
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
