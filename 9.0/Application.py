"""Explicit Flask application assembly, without starting background workers."""
from flask import Flask


def create_app(app: Flask | None = None) -> Flask:
    """Build routes and callbacks once per app, using the shared process state.

    When ``app`` is given, the rules are registered on that existing instance
    (Runtime creates it at import time, exactly like the monolith). With no
    argument, an independent Flask app is created for tests or embedding.
    This never creates an isolated proxy instance: sessions, settings, queues
    and caches still belong to runtime.
    """
    from . import Runtime as state

    if app is None:
        app = Flask(__name__, static_folder=None)
        app.config['MAX_CONTENT_LENGTH'] = state.MAX_BODY_BYTES
    app.before_request(state._normalize_request_origin)
    app.after_request(state._main_security_headers)

    app.add_url_rule('/.__s2l_hp', view_func=state._s2l_hp_endpoint, methods=['GET'])
    app.add_url_rule('/__s2l_reset_stats__', view_func=state._s2l_reset_stats_endpoint,
                     methods=['POST', 'OPTIONS'])
    app.add_url_rule('/__s2l_viewer_data__', view_func=state._s2l_viewer_data_endpoint,
                     methods=['GET', 'OPTIONS'])
    app.add_url_rule('/__s2l_viewer__', view_func=state._s2l_viewer_endpoint,
                     methods=['GET', 'OPTIONS'])
    app.add_url_rule(f'{state._EXT_PREFIX}/<path:extpath>', view_func=state.ext_asset,
                     methods=state._ALL_METHODS)
    # Rule var names mirror the monolith verbatim (wspath/tcppath/udppath,
    # while the views sign _wspath/_tcppath/_udppath — see MODULAR_MAP §6:
    # replicated quirk; opening these URLs in a plain browser tab 500s in
    # the monolith and must 500 here too during the match phase).
    app.add_url_rule('/__s2l_ws_ext__/<path:wspath>', view_func=state.ws_ext, methods=['GET'])
    app.add_url_rule('/__s2l_tcp__/<path:tcppath>', view_func=state.tcp_ext, methods=['GET'])
    app.add_url_rule('/__s2l_udp__/<path:udppath>', view_func=state.udp_ext, methods=['GET'])
    app.add_url_rule('/<path:path>', view_func=state.proxy, methods=state._ALL_METHODS)
    app.add_url_rule('/', defaults={'path': ''}, view_func=state.proxy,
                     methods=state._ALL_METHODS)
    return app
