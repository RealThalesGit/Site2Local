"""HTTP fallbacks for the WebSocket/TCP/UDP tunnel routes.

A real tunnel request is intercepted at the socket layer before Flask runs.
These views only answer ordinary HTTP requests (for example, someone opening
the URL in a browser tab) with the protocol explanation and a 426 status.
"""
from __future__ import annotations

from flask import Response


def ws_ext(_wspath: str) -> Response:
    return Response(
        "This path is a WebSocket tunnel — send Upgrade: websocket",
        status=426,
        headers={"Upgrade": "websocket"},
    )


def tcp_ext(_tcppath: str) -> Response:
    return Response("This path is a raw TCP tunnel — send Upgrade: websocket",
                    status=426, headers={"Upgrade": "websocket"})


def udp_ext(_udppath: str) -> Response:
    return Response("This path is a raw UDP tunnel — send Upgrade: websocket",
                    status=426, headers={"Upgrade": "websocket"})
