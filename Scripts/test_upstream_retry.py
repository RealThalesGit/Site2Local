"""Offline regressions for upstream retries and exception logging."""
import ast
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from flask import Flask

ROOT = Path(__file__).resolve().parents[1]


def load_helpers(path, state):
    tree = ast.parse((ROOT / path).read_text(encoding='utf-8'))
    tree.body = [n for n in tree.body
                 if not (isinstance(n, ast.ImportFrom) and n.level)]
    namespace = {'_s2l_state': state}
    exec(compile(tree, path, 'exec'), namespace)
    return namespace


class UpstreamRetryTests(unittest.TestCase):
    def setUp(self):
        self.empty = SimpleNamespace(headers={}, status_code=201, content=b'')
        self.full = SimpleNamespace(headers={}, status_code=201, content=b'result')
        self.session = SimpleNamespace(cookies={}, request=Mock(side_effect=[self.empty, self.full]))
        state = SimpleNamespace(
            _get_client_session=lambda: self.session,
            MAIN_HOST='example.com', SITE_URL='https://example.com',
            rewrite_origin=lambda *a: None, inject_csrf_headers=lambda *a: None,
            DEVICE='macintosh', _sanitize_ua=lambda ua: ua,
            TLS_VERIFY=True, _browser_cookies=lambda: {},
            UA_PROFILES={'macintosh': 'UA'}, TIMEOUT_READ=12, TIMEOUT_CONN=5,
            _flatten_cookiejar=lambda *a, **kw: {},
            _BODY_METHODS={'POST', 'PUT', 'PATCH', 'DELETE'},
            _SAFE_METHODS={'GET', 'HEAD'}, _is_cf_block=lambda *a: False,
        )
        self.request = load_helpers('Proxy/Upstream/Request.py', state)['_do_upstream']
        self.app = Flask(__name__)

    def call(self, method):
        with self.app.test_request_context('/', method=method):
            return self.request(method, 'https://example.com/create',
                                SimpleNamespace(req_headers={}, req_body=b'create'))

    def test_successful_empty_mutation_is_not_repeated(self):
        for method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            with self.subTest(method=method):
                self.session.request.reset_mock(side_effect=True)
                self.session.request.side_effect = [self.empty, self.full]
                self.assertIs(self.call(method), self.empty)
                self.assertEqual(self.session.request.call_count, 1)

    def test_empty_get_still_retries_and_uses_result(self):
        self.assertIs(self.call('GET'), self.full)
        self.assertEqual(self.session.request.call_count, 2)

    def test_explicit_empty_get_does_not_retry(self):
        self.empty.headers = {'Content-Length': '0'}
        self.assertIs(self.call('GET'), self.empty)
        self.assertEqual(self.session.request.call_count, 1)


class ExceptionLoggingTests(unittest.TestCase):
    def test_empty_exception_uses_class_name(self):
        short = load_helpers('HTTP/Sessions/Clients.py', SimpleNamespace())['_short_exc']
        self.assertEqual(short(TimeoutError()), 'TimeoutError')
        self.assertEqual(short(ValueError('  ')), 'ValueError')
        self.assertEqual(short(ValueError('first\nsecond')), 'first')
        self.assertEqual(short(RuntimeError('x' * 121)), 'x' * 120 + '\u2026')


if __name__ == '__main__':
    unittest.main()
