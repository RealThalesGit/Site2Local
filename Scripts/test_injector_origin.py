"""Message origin regression; requires Node.js to execute the browser mock."""
import ast
import json
from pathlib import Path
import shutil
import subprocess
import threading
from types import SimpleNamespace
import unittest


@unittest.skipUnless(shutil.which("node"), "Node.js required for JavaScript regression")
class InjectorOriginTests(unittest.TestCase):
    def test_cold_and_warm_frame_origins(self):
        path = Path(__file__).resolve().parents[1] / "Rewriting/Injector.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        tree.body = [n for n in tree.body
                     if not (isinstance(n, ast.ImportFrom) and n.level)]
        state = SimpleNamespace(
            _cdn_port_lock=threading.RLock(), _cdn_host_port={}, DONT_REWRITE=[],
            MAIN_HOST="poki.com", _EXT_PREFIX="/__s2l_ext__", PORT=8080,
            SITE_URL="https://poki.com")
        namespace = {"_s2l_state": state}
        exec(compile(tree, str(path), "exec"), namespace)
        script = namespace["_build_s2l_injector"]().decode()
        start = script.index("try{var _meO=")
        patch = script[start:script.index("function _rwHtmlStr", start)]
        javascript = r'''
const vm = require('node:vm');
const assert = require('node:assert/strict');
function check(name, origin, sender, frames, ports, expected) {
    const context = vm.createContext({URL,
        location: {hostname:'localhost', href:'http://localhost:8080/br/'},
        document: {querySelectorAll:()=>frames}, M:'poki.com', MS:'https:',
        MP:8080, HP:ports, EXT:'/__s2l_ext__'});
    vm.runInContext(`class MessageEvent {
        constructor(o,s){this._o=o;this.source=s;}
        get origin(){return this._o;}
    };` + PATCH, context);
    context.nativeOrigin = origin; context.sender = sender;
    assert.equal(vm.runInContext('new MessageEvent(nativeOrigin,sender).origin',
                                context), expected, name);
}
const cold = {location:{href:'http://localhost:8080/__s2l_ext__/game.example/index.html'}};
check('cold iframe', 'http://localhost:8080', cold, [], {}, 'https://game.example');
const cross = {get location(){throw Error('cross-origin');}};
check('redirected iframe before polling', 'http://localhost:8081', cross,
    [{contentWindow:cross,src:'http://localhost:8080/__s2l_ext__/game.example/index.html'}],
    {}, 'https://game.example');
check('warm iframe', 'http://localhost:8081', cross, [],
    {'game.example':8081}, 'https://game.example');
check('parent', 'http://localhost:8080', {}, [], {}, 'https://poki.com');
check('remote origin', 'https://other.example:8080', {}, [], {},
    'https://other.example:8080');
'''
        result = subprocess.run(
            [shutil.which("node"), "-e", "const PATCH=" + json.dumps(patch) + ";" + javascript],
            capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
