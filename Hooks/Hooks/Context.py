"""Site2Local: hook context, registry, GUI bridge and editor store."""
from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
import json



@dataclass
class HookContext:
    method:       str
    url:          str
    path:         str
    query:        str
    req_headers:  dict
    req_body:     bytes
    resp_status:  int   = 200
    resp_headers: dict  = field(default_factory=dict)
    resp_body:    bytes = b""
    resp_ct:      str   = "application/octet-stream"

    def json(self) -> object:
        return json.loads(self.resp_body)

    def set_json(self, obj: object) -> None:
        self.resp_body = json.dumps(obj, ensure_ascii=False).encode()
        self.resp_ct   = "application/json; charset=utf-8"
        self.resp_headers.pop("content-length", None)

    def req_json(self) -> object:
        return json.loads(self.req_body)

    def set_req_json(self, obj: object) -> None:
        self.req_body = json.dumps(obj, ensure_ascii=False).encode()
        self.req_headers["content-type"] = "application/json; charset=utf-8"
