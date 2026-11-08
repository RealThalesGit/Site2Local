"""Browser-side S2L runtime injector."""
from __future__ import annotations

import json
import re

from .. import Runtime as _s2l_state


def _build_s2l_injector() -> bytes:
    with _s2l_state._cdn_port_lock:
        # Only LIVE ports. A -1 entry is the "server starting" sentinel and
        # 0 means "routes via /__s2l_ext__/" — serializing the -1 made the
        # JS rewriter (truthiness checks) emit http://localhost:-1/... links
        # for pages served inside the registration race window. The
        # /.__s2l_hp live endpoint already filters this way.
        hp = {h: p for h, p in _s2l_state._cdn_host_port.items() if p > 0}
    # DONT_REWRITE — host patterns whose URLs must go DIRECT (never rewritten
    # to localhost). Compiled into one regex for the JS side so rw() can
    # short-circuit cheaply. Captcha backends (hcaptcha.com, recaptcha.net,
    # ...) reject requests whose Origin is "localhost", so their assets and
    # scripts must NOT be tunneled through S2L.
    _dr_patterns = []
    for _entry in _s2l_state.DONT_REWRITE:
        _e = _entry.strip().lower()
        if not _e:
            continue
        if "/" in _e:
            _h, _p = _e.split("/", 1)
            _dr_patterns.append(
                r'^(?:[^/]*\.)?' + re.escape(_h) + r'/' + re.escape(_p)
            )
        else:
            _dr_patterns.append(
                r'^(?:[^/]*\.)?' + re.escape(_e) + r'(?:[:/]|$)'
            )
    _dr_regex_js = json.dumps("|".join(_dr_patterns) if _dr_patterns else "")
    js = (
        '<script id="__s2l__">'
        '(function(){'
        'var M=' + json.dumps(_s2l_state.MAIN_HOST) + ';'
        'var HP=' + json.dumps(hp) + ';'
        'var EXT=' + json.dumps(_s2l_state._EXT_PREFIX) + ';'
        'var MP=' + json.dumps(_s2l_state.PORT) + ';'
        # MS: the MAIN target's real scheme ("http:" / "https:"). SP: the real
        # scheme of whatever document this script runs in — the main port
        # reflects the target's scheme; every other port is a CDN mini-server
        # whose upstream is always https (see _start_cdn_server). The
        # location.protocol/origin spoof below uses SP so any site, http or
        # https, keeps taking the same code path it takes on the original
        # (sites that pick ws://:plain-port vs wss://:443 by protocol).
        'var MS=' + json.dumps("http:" if str(_s2l_state.SITE_URL).lower().startswith("http://") else "https:") + ';'
        'var SP=(location.port===String(MP)||(!location.port&&MP===80))?MS:"https:";'
        'var PX="http://"+location.hostname+":"+MP;'
        'var DNR=' + _dr_regex_js + ';'
        'var DNR_RE=DNR?new RegExp(DNR,"i"):null;'
        # ── Shared core: rw() + fetch/XHR/WebSocket/Worker patches ─────────────
        # Standalone named function so .toString() can be re-injected inside
        # Workers. proxyPort is always the TRUE main port (MP), never the
        # current document's location.port — CDN sub-iframes run on :8087 etc.,
        # but /__s2l_ext__/ only exists on the main app.
        # v8.4 — signature now takes DNR (string source) instead of DNR_RE
        # (RegExp object) because JSON.stringify(RegExp) returns "{}" —
        # the worker then receives a truthy empty object, and DNR_RE.test()
        # throws TypeError. We compile the RegExp INSIDE the worker. Also
        # added scriptBase (7th arg): the original script's URL, used as
        # the base for resolving relative importScripts() args so
        # "./chunks/5376.js" from a script at /sub/worker.js resolves to
        # /sub/chunks/5376.js (NOT /chunks/5376.js at proxy root).
        'function __s2l_core(M,HP,EXT,proxyHost,proxyPort,DNR,scriptBase){'
          'var PX="http://"+proxyHost+":"+proxyPort;'
          # Recompile DNR_RE INSIDE the worker — DNR comes in as a source
          # string (or null), so this survives JSON serialization. The old
          # approach passed DNR_RE (a RegExp object) and JSON.stringify
          # turned it into "{}" → DNR_RE.test() threw inside rw().
          'var DNR_RE=(typeof DNR==="string"&&DNR.length>0)?new RegExp(DNR,"i"):null;'
          # scriptBase: the URL of the actual worker script (only meaningful
          # inside workers reached via the blob-prelude path — same-origin
          # workers skip the prelude and use native importScripts which
          # already resolves against self.location.href). null in main page.
          'var SB=typeof scriptBase==="string"&&scriptBase?scriptBase:null;'
          'function rw(u){'
          # Protocol-relative URLs (//cdn.host/path) → prepend https: BEFORE the
          # guard check, or they'd bypass the proxy and hit the real CDN.
          'if(typeof u==="string"&&u.length>2&&u[0]==="/"&&u[1]==="/")u="https:"+u;'
          'if(!u||typeof u!="string"||u[0]=="/"||u.indexOf("://")<0)return u;'
          'try{var p=new URL(u,"http://"+proxyHost+"/");'
          # DONT_REWRITE short-circuit: matching hosts (captcha widgets, etc.)
          # bypass rw() entirely — the browser connects directly.
          'if(DNR_RE&&DNR_RE.test(p.host+p.pathname))return u;'
          'if(p.hostname===proxyHost)return u;'
          # WS URLs must be handled BEFORE the MAIN_HOST check, or wss://MAIN_HOST
          # gets rewritten to http://localhost:PORT (invalid WS URL → SyntaxError).
          'if(p.protocol==="wss:"||p.protocol==="ws:"){'
            'var wh=p.host;'
            'var cdnp=Math.max(0,HP[p.hostname]||HP[p.host]||0);'
            'if(p.host===M||p.host==="www."+M)return "ws://"+proxyHost+":"+proxyPort+p.pathname+(p.search||"")+(p.hash||"");'
            'if(cdnp)return "ws://"+proxyHost+":"+cdnp+p.pathname+(p.search||"")+(p.hash||"");'
            'return "ws://"+proxyHost+":"+proxyPort+(p.protocol==="ws:"?"/__s2l_ws_ext_plain__/":"/__s2l_ws_ext__/")+wh+p.pathname+(p.search||"")+(p.hash||"");'
          '}'
          'if(p.host===M||p.host==="www."+M)return PX+p.pathname+(p.search||"")+(p.hash||"");'
          'if(HP[p.host]>0)return "http://"+proxyHost+":"+HP[p.host]+p.pathname+(p.search||"")+(p.hash||"");'
          'if(p.protocol==="https:"&&p.hostname!==proxyHost)return PX+EXT+"/"+p.host+p.pathname+(p.search||"")+(p.hash||"");'
          '}catch(e){}return u;}'
          'try{var _f=self.fetch;if(_f)self.fetch=function(i,o){'
            'if(typeof i==="string")i=rw(i);'
            'else if(i&&i.url){var r=rw(i.url);if(r!==i.url)i=new Request(r,i);}'
            'return _f.call(this,i,o);};'
          '}catch(e){}'
          'try{if(typeof XMLHttpRequest!=="undefined"){'
            'var _x=XMLHttpRequest.prototype.open;'
            'XMLHttpRequest.prototype.open=function(){'
              'var a=Array.prototype.slice.call(arguments);a[1]=rw(a[1]);'
              'return _x.apply(this,a);};'
          '}}catch(e){}'
          'try{var _WS=self.WebSocket;if(_WS){'
            'function S2LWebSocket(url,protos){'
              # Accept string / URL / Location inputs + options-object form
              # ({protocols: ...}) that some modern libraries use.
              'var ru=url;'
              'if(typeof url==="string")ru=rw(url);'
              'else if(url&&url.href)ru=rw(url.href);'
              'else if(url&&typeof url.toString==="function"){try{ru=rw(url.toString());}catch(e){}}'
              'if(protos&&typeof protos==="object"&&!Array.isArray(protos)&&protos.protocols)protos=protos.protocols;'
              'return protos?new _WS(ru,protos):new _WS(ru);'
            '}'
            'S2LWebSocket.prototype=_WS.prototype;'
            '["CONNECTING","OPEN","CLOSING","CLOSED"].forEach(function(k){S2LWebSocket[k]=_WS[k];});'
            'self.WebSocket=S2LWebSocket;'
          '}}catch(e){}'
          # Nested workers: rewrite the script URL only (no full prelude
          # re-injection — would cause unbounded blob-wrapping recursion).
          'try{var _W2=self.Worker;if(_W2){'
            'self.Worker=function(u,o){return new _W2(typeof u==="string"?rw(u):u,o);};'
            'self.Worker.prototype=_W2.prototype;'
          '}}catch(e){}'
          # importScripts() inside workers: schemeless args (webpack chunks)
          # were resolved against the blob: URL → "invalid URL". Resolve them
          # against scriptBase (the actual script's URL — preserves directory
          # context so "./5376.js" from /sub/worker.js → /sub/chunks/5376.js
          # NOT /chunks/5376.js at proxy root); schemed args go through rw().
          # v8.1 fix: the generic /^[a-z][a-z0-9+.\-]*:/ regex matched
          # "localhost:8080/..." as if "localhost:" were a URL scheme, sending
          # a schemeless-but-ported string through rw() which then mis-parsed
          # it as a host → produced "/__s2l_ext__/localhost:8080/__s2l_ext__/..."
          # (double-rewrite). Whitelist the real URL schemes instead.
          # v8.4 fix: use SB (scriptBase) as base for relative URLs — fixes
          # the 404 → "worker sent an error! undefined:undefined" symptom on
          # WASM-threaded web clients where cross-origin workers served from
          # a CDN port (http://localhost:8087/worker.js) did
          # importScripts("./chunks/5376.js") and got back
          # http://localhost:8080/chunks/5376.js (wrong port, 404 HTML
          # response, worker aborts).
          'try{var _is=self.importScripts;if(typeof _is==="function"){'
            'self.importScripts=function(){'
              'var args=Array.prototype.slice.call(arguments).map(function(u){'
                'if(typeof u!=="string")return u;'
                'if(/^(?:https?|wss?|blob|data|file|ftp):/i.test(u))return rw(u);'
                # Resolve relative URLs against scriptBase (the actual
                # worker script's URL — preserves directory context).
                # Only fall back to PX+"/" when SB is null (main-page call
                # to __s2l_core, where there's no enclosing script).
                'var base=SB||PX+"/";'
                'try{return new URL(u,base).href;}catch(e){'
                  # Fallback: manually construct an absolute URL. Strip leading
                  # "./" and "../" (one or more) so the path is clean, then
                  # prepend PX + "/". The old `PX+u` produced an invalid URL
                  # like "http://localhost:8080./next/..." (missing /).
                  'var cu=u.replace(/^(?:\\.\\.?\\/)+/,"");'
                  'return PX+"/"+cu;'
                '}'
              '});'
              'return _is.apply(self,args);'
            '};'
          '}}catch(e){}'
          'return rw;'
        '}'
        'var rw=__s2l_core(M,HP,EXT,location.hostname,MP,DNR,null);'
        # ── location.protocol / origin override ────────────────────────────────
        # Forces location.protocol="https:" so SPAs building URLs like
        # `"wss://"+location.host` or `location.protocol+"//"+host` don't produce
        # https://localhost:8080 → ERR_SSL_PROTOCOL_ERROR. The rw() wrappers
        # downgrade wss→ws and https→http internally. Side effect: sites that
        # check location.protocol to decide ws vs wss will pick wss — the proxy
        # then refuses upstream and caches the refusal (5 min) so retries are
        # silent. try/catch: location.protocol is non-configurable in some engines.
        'try{'
          'var _locP2=Object.getPrototypeOf(location);'
          'Object.defineProperty(_locP2,"protocol",{'
            'configurable:true,get:function(){return SP;}'
          '});'
          'Object.defineProperty(_locP2,"origin",{'
            'configurable:true,get:function(){'
              'return SP+"//"+location.host;'
            '}'
          '});'
        '}catch(e){}'
        # ── location.href / .assign() / .replace() / window.open() patch ───────
        # Catch raw navigations (location.href=..., .assign(), .replace(),
        # window.open()) — none of the fetch/XHR/src patches above intercept
        # these, so without rewriting the browser would connect to the REAL
        # host directly. All are configurable accessors/methods in major engines.
        'try{'
          'var _locP=Object.getPrototypeOf(location);'
          'var _lhD=Object.getOwnPropertyDescriptor(_locP,"href");'
          'if(_lhD&&_lhD.set){'
            'Object.defineProperty(_locP,"href",{'
              'get:_lhD.get,'
              'set:function(v){return _lhD.set.call(this,typeof v==="string"?rw(v):v);}'
            '});'
          '}'
          'var _lAssign=_locP.assign;'
          'if(typeof _lAssign==="function"){'
            '_locP.assign=function(v){return _lAssign.call(this,typeof v==="string"?rw(v):v);};'
          '}'
          'var _lReplace=_locP.replace;'
          'if(typeof _lReplace==="function"){'
            '_locP.replace=function(v){return _lReplace.call(this,typeof v==="string"?rw(v):v);};'
          '}'
        '}catch(e){}'
        'try{'
          'var _wOpen=window.open;'
          'if(typeof _wOpen==="function"){'
            'window.open=function(u,n,s){return _wOpen.call(this,typeof u==="string"?rw(u):u,n,s);};'
          '}'
        '}catch(e){}'
        # ── Window.prototype.postMessage patch ─────────────────────────────────
        # SDKs do postMessage(msg, 'https://real-origin') — browser drops it
        # because the receiver is localhost, never the expected origin; the SDK's
        # fallback then navigates to the real host → "connection refused". Fix:
        # relax any specific (non-"*") targetOrigin to "*". Safe locally because
        # no third untrusted party can intercept a wildcard message. Applies in
        # every realm, both directions, for any SDK.
        'try{'
          'var _pmD=Object.getOwnPropertyDescriptor(Window.prototype,"postMessage");'
          'if(!_pmD){'
            # Fallback: wrap window.postMessage directly
            'var _pm0=window.postMessage;'
            'window.postMessage=function(m,t,tr){'
              'if(typeof t==="string"&&t!=="*")t="*";'
              'return _pm0.apply(this,[m,t,tr]);'
            '};'
          '} else if(typeof _pmD.value==="function"){'
            'var _pm1=_pmD.value;'
            'Object.defineProperty(Window.prototype,"postMessage",{'
              'configurable:true,writable:true,enumerable:true,'
              'value:function(m,t,tr){'
                'if(typeof t==="string"&&t!=="*")t="*";'
                'return _pm1.apply(this,[m,t,tr]);'
              '}'
            '});'
          '}'
        '}catch(e){}'
        # ── MessageEvent.prototype.origin spoof ────────────────────────────────
        # After relaxing targetOrigin, the SDK still checks event.origin !==
        # 'https://expected-origin' and ignores the message. Patch the getter so
        # any origin served from OUR ports (main port or a CDN mini-server port)
        # maps back to the real domain: localhost:MP and <LAN-IP>:MP both →
        # "https://"+MAIN_HOST, :<cdn-port> → that CDN's real domain. Matching is
        # PORT-based (not hostname-based): remote devices reach the proxy through
        # a LAN IP, and a hostname whitelist would leave their MessageEvents
        # untranslated — breaking app↔embed SDK handshakes on every device except
        # the S2L host. Must NOT collapse every local port to MAIN_HOST: app↔embed
        # handshakes verify each other's CDN origin, and a mismatch makes some
        # wrappers re-point the embed iframe at the real remote host.
        'try{'
          'var _meO=Object.getOwnPropertyDescriptor(MessageEvent.prototype,"origin");'
          'if(_meO&&_meO.get&&_meO.configurable){'
            'var _meOg=_meO.get;'
            'Object.defineProperty(MessageEvent.prototype,"origin",{'
              'configurable:true,enumerable:true,'
              'get:function(){'
                'var o=_meOg.call(this);'
                'if(!o)return o;'
                'try{'
                  'var pu=new URL(o);'
                  'var pp=pu.port?parseInt(pu.port,10):(pu.protocol==="https:"?443:80);'
                  'if(pp===MP)return MS+"//"+M;'
                  'for(var h in HP){if(HP[h]===pp)return "https://"+h;}'
                '}catch(_e){}'
                'return o;'
              '}'
            '});'
          '}'
        '}catch(e){}'
        # ── innerHTML / outerHTML / insertAdjacentHTML / document.write patch ──
        # These hand a raw HTML string to the native parser, which sets src/href/
        # action attributes during parsing — that step never goes through the
        # .src/.setAttribute property patches (only JS-level access does). The
        # MutationObserver below catches them after parsing.
        #
        # We do NOT wrap the SETTERS themselves — that breaks React/Vue/Svelte
        # hydration. SPAs serialize their server-rendered DOM, then hydrate by
        # reading innerHTML / comparing it to the expected virtual DOM. Patching
        # the setter causes subtle differences (URL rewrites that produce
        # different strings than the framework expects) → hydration mismatch →
        # framework aborts → blank page on modern SPA sites. The
        # MutationObserver + the .src setter patches already
        # cover >99% of real-world cases. document.write is kept patched
        # because it's rare in modern apps and trivially safe (no hydration
        # concerns).
        'function _rwHtmlStr(html){'
          'try{'
            'return html.replace('
              '/((?:src|href|poster|data-src|data-href|action)\\s*=\\s*)(["\\x27])https?:\\/\\/([a-zA-Z0-9\\-._:]+)((?:(?!\\2)[^<>])*)\\2/gi,'
              'function(m,pre,q,host,tail){return pre+q+rw("https://"+host+tail)+q;}'
            ');'
          '}catch(e){return html;}'
        '}'
        'try{'
          'var _dw=document.write, _dwl=document.writeln;'
          'if(typeof _dw==="function")document.write=function(){'
            'var a=Array.prototype.slice.call(arguments).map(function(s){'
              'return typeof s==="string"?_rwHtmlStr(s):s;});'
            'return _dw.apply(document,a);'
          '};'
          'if(typeof _dwl==="function")document.writeln=function(){'
            'var a=Array.prototype.slice.call(arguments).map(function(s){'
              'return typeof s==="string"?_rwHtmlStr(s):s;});'
            'return _dwl.apply(document,a);'
          '};'
        '}catch(e){}'
        # ── Worker / SharedWorker constructor wrapping ──────────────────────────
        # Workers have their own global scope and don't inherit window.fetch —
        # without wrapping, their network calls bypass the proxy and hit the
        # real host (hung because hosts file points it at us). Worse, WASM-thread
        # apps that Atomics.wait() on a worker freeze the whole tab.
        #
        # BLOB WRAPPER ISSUE: The original approach wrapped every Worker in a
        # blob: URL to inject __s2l_core (fetch/XHR/URL rewriting). But a
        # blob: URL worker loses the original script's base URL, so relative
        # importScripts() calls (e.g. webpack's "./5376.js" for chunk loading)
        # can't be resolved — the browser throws "The URL '...' is invalid"
        # and the app freezes. This is the WASM-game freeze bug.
        #
        # FIX: For SAME-ORIGIN scripts (already on localhost:PORT), skip the
        # blob wrapper and create the Worker directly with the rewritten URL.
        # Relative importScripts URLs then resolve against the script's real
        # URL (e.g. /_next/static/chunks/7065.js → ./5376.js resolves to
        # /_next/static/chunks/5376.js — correct). The trade-off is that
        # same-origin workers don't get __s2l_core's fetch/XHR patches, but
        # same-origin requests already go through the proxy by default (the
        # browser sends them to localhost:PORT). Cross-origin workers still
        # get the blob wrapper + __s2l_core + importScripts URL resolution.
        '[["Worker","__s2l__worker_orig"],["SharedWorker","__s2l__sharedworker_orig"]].forEach(function(pair){'
          'var Ctor=window[pair[0]];if(!Ctor)return;'
          'function Wrapped(scriptURL,opts){'
            'var abs;try{abs=new URL(scriptURL,location.href).href;}catch(e){abs=scriptURL;}'
            'var ru=typeof abs==="string"?rw(abs):abs;'
            # Only prefix PX when ru has NO URL scheme (true bare/relative).
            # The old `ru.indexOf("http")!==0` check wrongly prefixed blob:/data:
            # URLs → "http://localhost:8080blob:http://..." → invalid → every
            # WASM/audio worker failed to start.
            # v8.1: whitelist real URL schemes — same fix as importScripts.
            'if(typeof ru==="string"&&!/^(?:https?|wss?|blob|data|file|ftp):/i.test(ru))ru=PX+ru;'
            # v8.2 — BLOB/DATA workers: pass through directly, do NOT wrap in
            # a blob prelude. importScripts() refuses blob:/data: URLs in
            # Chrome/Firefox (only http/https/same-origin supported), so the
            # prelude's `importScripts("blob:...")` throws a NetworkError
            # BEFORE the worker's own code runs. The parent's worker.onerror
            # then receives an Event with no message/filename/lineno (the
            # classic "worker sent an error! undefined:undefined: undefined"
            # symptom that breaks WASM-threaded web clients that build
            # workers from inline Blob sources).
            # The blob's code was already constructed by the (patched) main
            # page, so its URLs went through rw() at construction time.
            'if(typeof ru==="string"&&/^(?:blob|data):/i.test(ru)){'
              'return new Ctor(ru,opts);'
            '}'
            # v8.4 — MODULE workers: `new Worker(url, {type:"module"})`.
            # Module workers use ES module semantics — they import via
            # `import` statements, NOT importScripts(). The blob prelude
            # below uses importScripts() which THROWS TypeError in module
            # workers → worker aborts with empty onerror Event → the same
            # "worker sent an error! undefined:undefined" symptom. Skip
            # the prelude entirely for module workers — they fetch their
            # own imports through the proxy (the browser's import
            # resolution uses URLs that go through rw() via fetch patch
            # on the main page; module workers fetch cross-origin via
            # CORS-mode which S2L already supports).
            'if(opts&&typeof opts==="object"&&typeof opts.type==="string"&&opts.type.toLowerCase()==="module"){'
              'return new Ctor(ru,opts);'
            '}'
            # Same-origin check: if the rewritten URL is on the proxy origin
            # (http://localhost:PORT/...), create the Worker directly WITHOUT
            # a blob wrapper. This preserves the script's real URL so relative
            # importScripts() calls (webpack chunk loading) resolve correctly.
            # The importScripts patch in __s2l_core resolved relative URLs
            # against PX+"/" (proxy root), which produced WRONG paths for
            # chunk-relative URLs like "./5376.js" → "/5376.js" instead of
            # "/_next/static/chunks/5376.js". Creating the Worker directly
            # fixes this because the browser resolves relative URLs against
            # the script's actual URL.
            'if(typeof ru==="string"&&ru.indexOf(PX+"/")===0){'
              'return new Ctor(ru,opts);'
            '}'
            # Cross-origin worker: use blob wrapper to inject __s2l_core.
            # The importScripts patch inside __s2l_core resolves relative
            # URLs against scriptBase (the worker's own URL, passed as 7th
            # arg to __s2l_core) so "./chunks/5376.js" from a script at
            # http://localhost:8087/worker.js resolves to
            # http://localhost:8087/chunks/5376.js (CORRECT port + path),
            # NOT http://localhost:8080/chunks/5376.js (wrong port, 404).
            # v8.4 fix: pass DNR (string source) instead of DNR_RE (RegExp
            # object) — JSON.stringify(RegExp) returns "{}" which broke
            # the worker's DNR short-circuit. The worker recompiles DNR
            # into a RegExp via new RegExp(DNR, "i") inside __s2l_core.
            'try{'
              'var prelude="("+__s2l_core.toString()+")("+JSON.stringify(M)+","+JSON.stringify(HP)+","+'
                'JSON.stringify(EXT)+","+JSON.stringify(location.hostname)+","+JSON.stringify(MP)+","+JSON.stringify(DNR)+","+JSON.stringify(ru)+");\\n"'
                '+"importScripts("+JSON.stringify(ru)+");";'
              'var blob=new Blob([prelude],{type:"application/javascript"});'
              'var blobURL=URL.createObjectURL(blob);'
              'return new Ctor(blobURL,opts);'
            '}catch(e){return new Ctor(ru,opts);}'
          '}'
          'Wrapped.prototype=Ctor.prototype;'
          'window[pair[0]]=Wrapped;'
        '});'
        # ── src setter patch (Image/Script/IFrame/Video/Source) ────────────────
        # Catches dynamically-created elements whose .src is set via JS before
        # insertion in the DOM (MutationObserver misses them — not yet in DOM).
        'try{'
          '[["HTMLImageElement","img"],["HTMLScriptElement","script"],'
          '["HTMLIFrameElement","iframe"],["HTMLMediaElement","media"],'
          '["HTMLSourceElement","source"]].forEach(function(pair){'
            'var ctor=window[pair[0]];if(!ctor||!ctor.prototype)return;'
            'var d=Object.getOwnPropertyDescriptor(ctor.prototype,"src");'
            'if(!d||!d.set)return;'
            'Object.defineProperty(ctor.prototype,"src",{'
              'get:d.get,'
              'set:function(v){'
                'var u=typeof v==="string"?v:(v&&typeof v.href==="string"?v.href:v);'
                'return d.set.call(this,typeof u==="string"?rw(u):u);'
              '}'
            '});'
          '});'
        '}catch(e){}'
        # setAttribute() starts the fetch as soon as a script element is
        # connected. Rewriting it synchronously avoids racing that request
        # against the MutationObserver for SDKs that build script tags by hand.
        'try{'
          'var _elSA=Element.prototype.setAttribute;'
          'Element.prototype.setAttribute=function(name,value){'
            'var n=typeof name==="string"?name.toLowerCase():"";'
            'if((n==="src"||n==="href"||n==="poster"||n==="data-src"||n==="data-href"||n==="action")'
               '&&(typeof value==="string"||(value&&typeof value.href==="string"))){'
              'var u=typeof value==="string"?value:value.href;'
              'value=rw(u);'
            '}'
            'return _elSA.call(this,name,value);'
          '};'
        '}catch(e){}'
        # ── HTMLIFrameElement.setAttribute("src", ...) patch ────────────────────
        # Some frameworks use setAttribute instead of the .src property.
        'try{'
          'var _ifSA=HTMLIFrameElement.prototype.setAttribute;'
          'HTMLIFrameElement.prototype.setAttribute=function(name,value){'
            'if(name&&name.toLowerCase()==="src"&&typeof value==="string"&&value.indexOf("http")===0)value=rw(value);'
            'return _ifSA.call(this,name,value);'
          '};'
        '}catch(e){}'
        # ── CSS backgroundImage + cssText setter patch ────────────────────────
        # Catches: element.style.backgroundImage = "url(https://cdn.example.com/bg.png)"
        'try{'
          'var _cssRw=function(v){'
          'if(typeof v!=="string")return v;'
          # Quoted first: url("...") / url('...') — body may contain parens.
          'v=v.replace(/url\\(\\s*(["\'])(https?:\\/\\/[^"\']+)?\\1\\s*\\)/gi,function(m,q,u){var r=rw(u||"");return r&&r!==u?"url(\\""+r+"\\")":m;});'
          # Unquoted: ONE level of balanced parens in the body (paths like
          # %20(3)%20(1).png), so the match can never overshoot the url()'s
          # own closing ')'. The trailing-trim stays as belt-and-suspenders.
          'v=v.replace(/url\\(\\s*(https?:\\/\\/[^()"\'\\s]*(?:\\([^()"\'\\s]*\\)[^()"\'\\s]*)*)\\s*\\)/gi,function(m,u){var r=rw(u);if(r){while(r.charCodeAt(r.length-1)===41&&r.split("(").length<r.split(")").length)r=r.slice(0,-1);}return r&&r!==u?"url(\\""+r+"\\")":m;});'
          'return v;'
          '};'
          'var _bgD=Object.getOwnPropertyDescriptor(CSSStyleDeclaration.prototype,"backgroundImage");'
          'if(_bgD&&_bgD.set){'
            'Object.defineProperty(CSSStyleDeclaration.prototype,"backgroundImage",{'
              'get:_bgD.get,set:function(v){return _bgD.set.call(this,_cssRw(v));}'
            '});'
          '}'
          'var _ctD=Object.getOwnPropertyDescriptor(CSSStyleDeclaration.prototype,"cssText");'
          'if(_ctD&&_ctD.set){'
            'Object.defineProperty(CSSStyleDeclaration.prototype,"cssText",{'
              'get:_ctD.get,set:function(v){return _ctD.set.call(this,_cssRw(v));}'
            '});'
          '}'
        '}catch(e){}'
        # ── DOM attribute scanner + MutationObserver ──────────────────────────
        # try/catch: document.documentElement can be null in sandboxed/srcdoc
        # iframes — .observe(null) would throw and halt all subsequent patches.
        'try{'
        'var _A=["src","href","poster","data-src","action"];'
        'function _rn(n){if(!n||n.nodeType!==1)return;'
        '_A.forEach(function(a){var v=n.getAttribute(a);if(v&&v.indexOf("http")===0){var r=rw(v);if(r!==v)n.setAttribute(a,r);}});'
        'var ss=n.getAttribute("srcset");'
        'if(ss){var rs=ss.split(/,\\s+/).map(function(e){var b=e.trim().split(/ +/);if(b[0]&&b[0].indexOf("http")===0){var r=rw(b[0]);if(r!==b[0])b[0]=r;}return b.join(" ");}).join(", ");if(rs!==ss)n.setAttribute("srcset",rs);}}'
        'function _ra(r){try{var e=r.querySelectorAll("[src],[href],[srcset],[poster],[data-src],[action]");for(var i=0;i<e.length;i++)_rn(e[i]);}catch(x){}}'
        # attributeFilter catches src/href/poster changes via setAttribute OR
        # reflected property setters (a.href=..., video.poster=...) we didn't
        # patch individually. rw() is idempotent on proxied/relative URLs, so
        # re-triggering this observer is a harmless no-op.
        'new MutationObserver(function(ms){ms.forEach(function(m){'
          'if(m.type==="attributes"){_rn(m.target);}'
          'else{m.addedNodes.forEach(function(n){_rn(n);_ra(n);});}'
        '});}).observe(document.documentElement,{'
          'childList:true,subtree:true,'
          'attributes:true,attributeFilter:["src","href","poster","data-src","srcset","action"]'
        '});'
        'if(document.readyState!=="loading")_ra(document);else document.addEventListener("DOMContentLoaded",function(){_ra(document);});'
        '}catch(e){}'
        # ── HTMLFormElement.prototype.submit() patch ────────────────────────────
        # form.submit() from JS bypasses click-driven navigation and the action
        # attribute may have changed since the scanner last ran.
        'try{'
          'var _formSubmit=HTMLFormElement.prototype.submit;'
          'HTMLFormElement.prototype.submit=function(){'
            'var a=this.getAttribute("action");'
            'if(a&&a.indexOf("http")===0){var r=rw(a);if(r!==a)this.setAttribute("action",r);}'
            'return _formSubmit.call(this);'
          '};'
        '}catch(e){}'
        # ── HTMLAnchorElement.prototype.click() patch ────────────────────────
        # JS that builds an <a href="https://real/..."> and calls a.click()
        # SYNCHRONOUSLY (the classic JS-driven installer/download flow does
        # exactly this) races the MutationObserver: the browser navigates before the
        # observer microtask rewrites the attribute → the request leaves the
        # proxy for the real host (Chrome then blocks the file download as
        # "redirected through an insecure connection"). Rewriting inside
        # click() itself is synchronous and cannot race.
        'try{'
          'var _aClick=HTMLAnchorElement.prototype.click;'
          'HTMLAnchorElement.prototype.click=function(){'
            'var h=this.getAttribute("href");'
            'if(h&&h.indexOf("http")===0){var r=rw(h);if(r!==h)this.setAttribute("href",r);}'
            'return _aClick.call(this);'
          '};'
        '}catch(e){}'
        # ── iframe.srcdoc patch ─────────────────────────────────────────────────
        # srcdoc embeds a full HTML document as a literal string — never passes
        # through server-side HTML rewriting and srcdoc isn't a URL attribute.
        # Rewrite any src/href/poster/data-src URLs inside the string before
        # assignment so the parsed iframe already points at the proxy.
        'try{'
          'var _sdD=Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype,"srcdoc");'
          'if(_sdD&&_sdD.set){'
            'Object.defineProperty(HTMLIFrameElement.prototype,"srcdoc",{'
              'get:_sdD.get,'
              'set:function(html){'
                'if(typeof html==="string"){'
                  'html=html.replace(/((?:src|href|poster|data-src)\\s*=\\s*)(["\'])(https?:\\/\\/[^"\']+)\\2/gi,'
                    'function(m,attr,q,u){var r=rw(u);return r!==u?attr+q+r+q:m;});'
                '}'
                'return _sdD.set.call(this,html);'
              '}'
            '});'
          '}'
        '}catch(e){}'
        # ── Service worker + cache handling ─────────────────────────────────
        # Do NOT blanket-unregister SWs or nuke caches — SPAs rely on their SW
        # for JS bundle caching and wiping hangs SPA init (blank page). The
        # fetch/XHR/WS patches already handle URL rewriting. Only unregister SWs
        # whose scope is NOT the current origin (stale from a previous target).
        'try{'
          'if(navigator.serviceWorker){'
            'navigator.serviceWorker.getRegistrations().then(function(r){'
              'r.forEach(function(s){'
                'try{'
                  'var sw=new URL(s.scope);'
                  'if(sw.hostname!==location.hostname){s.unregister();}'
                '}catch(e){}'
              '});'
            '}).catch(function(){});'
          '}'
        '}catch(e){}'
        # ── Live HP refresh — picks up CDN hosts registered after page load ───
        # Polls PX+/__s2l_hp every 2s (then 10s after 30 ticks) so CDN hosts
        # registered AFTER the initial page serve are picked up without a reload.
        '(function(){'
          'var _ht=0;'
          'function _hp_poll(){'
            '_ht++;'
            'fetch(PX+"/.__s2l_hp",{cache:"no-store"})'
              '.then(function(r){return r.json();})'
              '.then(function(d){for(var h in d)if(!HP[h]||HP[h]!==d[h])HP[h]=d[h];})'
              '.catch(function(){});'
            'setTimeout(_hp_poll,_ht<30?2000:10000);'
          '}'
          'setTimeout(_hp_poll,1500);'
        '})();'
        '})();</script>'
    )
    return js.encode("utf-8")
