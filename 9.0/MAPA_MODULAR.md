# MAPA_MODULAR — Guia da Modularização por Match

> **Site2Local** · **versão 9.0** · este documento é o mapa entre a árvore
> modular e o monolito `s2l.py` (13.705 linhas), mantido **na mesma pasta
> do pacote** como referência. **Cada símbolo da árvore modular aponta para
> a linha do monolito de onde saiu** — é a sua bússola para continuar o
> trabalho de *match* (comparar módulo ↔ monolito durante a transição).

---

## 0. Release 9.0 (08/11/2026) — a linha modular ganha número próprio

A versão agora vive em **um lugar só**: `Config.VERSION` (o banner do boot
lê de lá). A linha **9.x é a árvore modular**; o monólito congelado `s2l.py`
morce em 8.1 como referência histórica do match — as únicas divergências
deliberadas são as correções pós-match (seção 6) e o banner de versão.

**O que a 9.0 inaugura** (tudo verificado por bateria própria):
- migração completa monólito→modular (auditoria final: config 27/27 ·
  rotas 10/10 · smoke 9/9 · símbolos 193/0 perdidos · constantes 196/0
  perdidas · injetor +241 bytes = só os fixes 3b/4);
- correção 1-2: revalidação de cache por perfil de dispositivo + client
  hints coerentes (19/19);
- correção 3a-3b: isolamento COOP/COEP por engine (fim do
  `NS_ERROR_DOM_CORP_FAILED` no Firefox) + patch de `setProperty`
  (15/15);
- correção 4: fim do `localhost:-1` no resumo/injetor (18/18);
- correção 5: resgate de MIME por conteúdo — a bola preta do globo
  (46/46 + E2E com paridade visual pixel-a-pixel);
- **Princípio da universalidade** codificado: nenhuma correção amarrada a
  site; site-alvo é INPUT, bug de campo é detector de CLASSE.

---

## 1. Arquitetura

| Decisão | Como ficou |
|---|---|
| **`__init__.py`** | **Exatamente 1** no projeto inteiro: `Site2Local/__init__.py`. Todas as subpastas são *namespace packages* (PEP 420) — funcionam sem `__init__`. |
| **Hub de estado** | `Runtime.py` mantém todo o estado compartilhado e importa cada módulo folha **direto** (`from .Core.URL.Format import ...`), sem passar por `__init__` intermediário. |
| **Compatibilidade** | `import s2l` / `Site2Local.runtime` / `Site2Local.config` continuam funcionando via aliases no `__init__.py` raiz. |
| **Layout** | **Pasta única**: a raiz do repositório É a pasta do pacote `Site2Local/` — módulos, `s2l.py`, `Boot.py`, `requirements.txt` e docs convivem no mesmo path. |
| **Monolito** | `s2l.py` segue dentro da pasta, **intocado e executável** (`python Site2Local/s2l.py`, ou `python s2l.py` de dentro), para comparação lado a lado. |
| **Execução** | `python3 Site2Local/Boot.py` (da pasta-mãe) ou `python3 Boot.py` (de dentro) · equivalentes: `python -m Site2Local` (da pasta-mãe) e `python s2l.py` (monolito). Dados de runtime (`site_src/`, `site_data/`) ficam sempre **dentro** da pasta. |

### Por que 1 `__init__.py` só?

Os 33 `__init__.py` antigos faziam duas coisas: 32 eram marcadores vazios ou
re-exports mecânicos (X importa de Y só para o Runtime reimportar). Com
*namespace packages*, a pasta existe como pacote sem arquivo nenhum, e o
Runtime importa direto da folha — um nível indireto a menos, zero mágica,
e o `git grep` encontra qualquer símbolo em exatamente um lugar.

### Regra de ouro para continuar o match

> Ao mover/criar código, atualize este mapa: o símbolo novo ganha a linha
> do monolito de onde veio; símbolo sem origem é marcado **novo**.

---

## 2. Fragmentação desta revisão (os 3 gigantes)

| Arquivo antes | Depois | Motivo da costura |
|---|---|---|
| `Hooks/GUI.py` (1.790) | `Hooks/GUI/App.py` (1.720) + `Codecs.py` + `Find.py` | O Qt é um único *closure* (espelha a função única `_launch_hook_gui` do monolito) — só os helpers puros (hex/codec/find) são extraíveis sem risco. **App.py fica grande por decisão**: destrinchar o closure exigiria reescrever 1.7k linhas de Qt sem poder testar. *Evidência (scan AST)*: das 40 funções aninhadas de `_gui_main`, **0** têm zero variáveis de closure — todas tocam widgets (`tree`, `body_box`, …) ou imports Qt locais; não há mais nada extraível com diff zero. |
| `Viewers/CDN.py` (1.082) | `Viewers/CDN/Page.py` (template) + `Endpoint.py` | 1.058 linhas eram um único string HTML. O template saiu **byte-idêntico** (verificado contra o monolito) e a lógica virou 24 linhas. |
| `Proxy/Routes.py` (896) | `Proxy/Routes/{Main, Guards, Fetch, Recovery, Fallbacks}.py` | O `proxy()` era uma função de 875 linhas. Agora `Main.py` é um pipeline de ~250 linhas com **12 fases numeradas**; cada fase virou helper testável. |
| `Rewriting/Injector.py` (588) | `Rewriting/Injector/Build.py` (34) + `Template.py` (573) | O `js = ( ... )` de 554 linhas saiu **verbatim** para `Template.py` — só o cabeçalho da função mudou (`hp`/`_dr_regex_js` viraram parâmetros). **Verificado: injetor byte-idêntico** (11.604 bytes no fechamento do match; hoje 11.815 — a correção pós-match nº 3b acrescentou o patch de `setProperty`, ver seção 6). |

**Verificações executadas** (tudo passou):
- `py_compile` em 97 módulos
- import completo da árvore (com Flask real)
- smoke test OFFLINE: `/` 404, `/.__s2l_hp` 200, `/__s2l_viewer__` 200 (63 KB), `/__s2l_viewer_data__` 200, `/__s2l_reset_stats__` 200, OPTIONS 204
- smoke test ONLINE: `GET /` → 200, 140 KB baixados do upstream, injector `__s2l__` aplicado, cache salvo em `site_src/`
- roundtrip dos Codecs extraídos (`_bytes_to_hexdump` ↔ `_hexdump_to_bytes`, detecção de encoding)
- **bateria de paridade monolito ≡ modular** (símbolos AST + rotas + smoke lado a lado — seção 6)
- **injetor `__s2l__` byte-idêntico**: `_build_s2l_injector()` do monolito × do modular → 11.604 bytes iguais (após a fragmentação de `Injector/`; hoje 11.815 no modular — correção pós-match nº 3b, ver seção 6)
- **layout de pasta única**: boots reais nos 3 pontos de entrada (`Site2Local/Boot.py` da pasta-mãe, `Boot.py` de dentro, `python -m Site2Local`) — servidor no ar e `site_src/`/`site_data/` sempre **dentro** da pasta; `import s2l` histórico funciona de dentro

### Origem das novas costuras no monolito

Os helpers criados pela fragmentação são código **verbatim** de trechos do
`proxy()` do monolito — cada um preserva o match com a faixa de linhas de
onde saiu:

| Novo símbolo | Veio de `s2l.py` (dentro de `def proxy()`, linha 12499) |
|---|---|
| `_guard_preflight` | linhas ~12520-12581 (prefetch / SW / blocked / OPTIONS) |
| `_offline_response` | linhas ~12583-12641 (branch OFFLINE completo) |
| `_decide_streaming` | linhas ~12689-12713 (heurísticas de stream/SSE) |
| `_fetch_with_retries` | linhas ~12715-12801 (escada de retry + fallback de cache) |
| `_push_redirect_history` | linhas ~12802-12824 (histórico de redirects no GUI) |
| `_maybe_stream` | linhas ~12826-12876 (hand-off SSE / streaming) |
| `_consume_upstream_body` | linhas ~12878-12909 (decompress + recuperação stream vazio) |
| `_waf_block_retry` | linhas ~12911-12965 (detecção de bloco WAF + retry) |
| `_handle_304` | linhas ~12966-12997 (304 sem validadores) |
| `_recover_empty_body` | linhas ~12998-13100 (corpos vazios A/B/C) |
| `_cdn_fallback` | linhas ~13101-13171 (fallback de hosts CDN) |
| `_rsc_refetch` | linhas ~13186-13217 (payload RSC → HTML completo) |
| `_bot_page_response` | linhas ~13236-13262 (página de bot → 503) |
| `_save_and_enqueue` | linhas ~13264-13299 (salvar no cache + discovery) |
| `_rewrite_resp_body` | linhas ~13301-13321 (pós-processamento HTML/CSS/JSON) |
| `_final_blank_page_recovery` | linhas ~13328-13388 (guarda de página em branco) |
| `_bytes_to_hexdump` / `_hexdump_to_bytes` / `_ws_auto_encoding` | dentro de `_launch_hook_gui` (linha 1196) |
| `_find_all` | dentro de `_launch_hook_gui` (linha 1196) — ganhou o param `highlight_hex` (era a constante `_YLW_HI` do closure) |
| `_CDN_VIEWER_PAGE` (Page.py) | string inline de `_s2l_viewer_endpoint` (linha ~11020), byte-idêntica |

---

## 3. Árvore completa (pasta única: 98 módulos · 15750 linhas · 1 `__init__.py`)

A raiz do repositório **é** a pasta do pacote — código, monolito de referência,
lançador, requirements e docs convivem no mesmo path:

```
Site2Local/
├── Capture/
│   ├── HTTP/
│   │   ├── Record.py
│   │   └── Worker.py
│   └── WebSocket/
│       ├── Record.py
│       └── Worker.py
├── CDN/
│   ├── Registry/
│   │   ├── Hosts.py
│   │   └── Origins.py
│   └── Server.py
├── CLI/
│   └── Banner.py
├── Core/
│   ├── Cache/
│   │   ├── Legacy.py
│   │   ├── Migration.py
│   │   ├── Naming.py
│   │   └── Paths.py
│   ├── Logging/
│   │   ├── Logger.py
│   │   └── Lolcat.py
│   ├── MIME/
│   │   ├── Assets.py
│   │   ├── ContentType.py
│   │   └── Guess.py
│   ├── Stats/
│   │   ├── CDNHealth.py
│   │   └── Model.py
│   ├── URL/
│   │   ├── Format.py
│   │   ├── Network.py
│   │   ├── Platform.py
│   │   ├── Policy.py
│   │   ├── Request.py
│   │   └── Rewrite.py
│   └── Compression.py
├── Crawler/
│   ├── Crawl/
│   │   ├── Assets.py
│   │   ├── Hosts.py
│   │   ├── Jobs.py
│   │   ├── Links.py
│   │   └── Save.py
│   └── Scanner/
│       ├── Runner.py
│       ├── Status.py
│       └── Wordlists.py
├── Hooks/
│   ├── GUI/
│   │   ├── App.py
│   │   ├── Codecs.py
│   │   └── Find.py
│   └── Hooks/
│       ├── Context.py
│       ├── GUIHooks.py
│       ├── GUITraffic.py
│       ├── GUIWS.py
│       ├── Registry.py
│       └── Store.py
├── HTTP/
│   ├── Headers/
│   │   ├── CI.py
│   │   ├── Cookies.py
│   │   ├── CORS.py
│   │   ├── Forwarding.py
│   │   ├── Isolation.py
│   │   ├── Response.py
│   │   └── Security.py
│   ├── Sessions/
│   │   ├── Clients.py
│   │   ├── Cloudflare.py
│   │   ├── Devices.py
│   │   └── Factory.py
│   └── Streaming/
│       ├── Detect.py
│       ├── Response.py
│       └── SSE.py
├── Network/
│   ├── Server/
│   │   ├── Handler.py
│   │   └── WSGI.py
│   └── Tunnels.py
├── Proxy/
│   ├── Cache/
│   │   ├── Range.py
│   │   ├── Serve.py
│   │   └── Validators.py
│   ├── Routes/
│   │   ├── Fallbacks.py
│   │   ├── Fetch.py
│   │   ├── Guards.py
│   │   ├── Main.py
│   │   └── Recovery.py
│   ├── Upstream/
│   │   ├── Headers.py
│   │   ├── Request.py
│   │   └── Response.py
│   ├── Assets.py
│   └── TunnelRoutes.py
├── Rewriting/
│   ├── HTML/
│   │   ├── Policy.py
│   │   ├── Reveal.py
│   │   ├── ServiceWorker.py
│   │   └── URLs.py
│   ├── Injector/
│   │   ├── Build.py
│   │   └── Template.py
│   └── JSON.py
├── State/
├── Viewers/
│   ├── CDN/
│   │   ├── Endpoint.py
│   │   └── Page.py
│   ├── Common.py
│   ├── Data.py
│   ├── Hosts.py
│   └── Reset.py
├── WebSockets/
│   ├── Protocol/
│   │   ├── Compression.py
│   │   ├── Frames.py
│   │   └── Hooks.py
│   ├── Upstream/
│   │   ├── CFFI.py
│   │   ├── Connect.py
│   │   └── Raw.py
│   └── Tunnel.py
├── Application.py
├── Boot.py
├── Config.py
├── MAPA_MODULAR.md
├── README.md
├── requirements.txt
├── Runtime.py
├── s2l.py
├── .gitignore
├── __init__.py
└── __main__.py
```

---

## 4. Tabela de match — símbolo → linha do `s2l.py`

> Gerada automaticamente: 215 símbolos de nível de módulo, **193 com match
> direto** no monolito, 22 marcados como **novo** (costuras criadas pela
> fragmentação desta revisão: fases do `proxy()`, helpers de GUI e o
> `create_app`). Símbolos dentro de closures (ex.: funções aninhadas de
> `_launch_hook_gui`) não aparecem — o match delas é o da função externa.

### `Application` (37 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def create_app` | **novo** (costura da fragmentação) |

### `CDN.Registry.Hosts` (59 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _register_cdn_host` | linha 6831 (`def _register_cdn_host`) |
| `def _preregister_cdn_hosts_from_disk` | linha 6846 (`def _preregister_cdn_hosts_from_disk`) |

### `CDN.Registry.Origins` (133 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _real_origin_for` | linha 6479 (`def _real_origin_for`) |
| `def _proxy_target` | linha 6875 (`def _proxy_target`) |

### `CDN.Server` (307 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _start_cdn_server` | linha 6540 (`def _start_cdn_server`) |

### `CLI.Banner` (138 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _banner` | linha 13380 (`def _banner`) |

### `Capture.HTTP.Record` (71 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _CaptureRecord` | linha 7654 (`class _CaptureRecord`) |
| `def _capture_path` | linha 7668 (`def _capture_path`) |
| `def _encode_body` | linha 7702 (`def _encode_body`) |

### `Capture.HTTP.Worker` (62 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _capture_worker` | linha 7708 (`def _capture_worker`) |
| `def _maybe_capture` | linha 7739 (`def _maybe_capture`) |

### `Capture.WebSocket.Record` (33 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _WSCaptureRecord` | linha 7766 (`class _WSCaptureRecord`) |
| `def _ws_capture_dir` | linha 7778 (`def _ws_capture_dir`) |

### `Capture.WebSocket.Worker` (63 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _ws_capture_worker` | linha 7783 (`def _ws_capture_worker`) |
| `def _maybe_capture_ws` | linha 7814 (`def _maybe_capture_ws`) |

### `Core.Cache.Legacy` (97 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _legacy_cache_paths` | linha 5326 (`def _legacy_cache_paths`) |

### `Core.Cache.Migration` (82 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _migrate_legacy_cache` | linha 5428 (`def _migrate_legacy_cache`) |

### `Core.Cache.Naming` (90 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _safe_seg` | linha 5230 (`def _safe_seg`) |
| `def _sanitize_query_for_filename` | linha 5243 (`def _sanitize_query_for_filename`) |
| `def _encode_query_for_filename` | linha 5274 (`def _encode_query_for_filename`) |

### `Core.Cache.Paths` (103 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def local_path` | linha 5503 (`def local_path`) |

### `Core.Compression` (201 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def decompress_body` | linha 3466 (`def decompress_body`) |
| `def _decompress_body_impl` | linha 3482 (`def _decompress_body_impl`) |

### `Core.Logging.Logger` (30 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def log` | linha 250 (`def log`) |

### `Core.Logging.Lolcat` (79 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _lolcat_internal` | linha 175 (`def _lolcat_internal`) |
| `def _probe_lolcat` | linha 197 (`def _probe_lolcat`) |
| `def _lolcat_real` | linha 227 (`def _lolcat_real`) |

### `Core.MIME.Assets` (36 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def is_static_asset` | linha 3752 (`def is_static_asset`) |
| `def is_subresource` | linha 3769 (`def is_subresource`) |

### `Core.MIME.ContentType` (41 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _ensure_wasm_content_type` | linha 3677 (`def _ensure_wasm_content_type`) |
| `def _ct_base` | linha 3720 (`def _ct_base`) |

### `Core.MIME.Guess` (53 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def guess_mime` | linha 3660 (`def guess_mime`) |
| `def _resolve_mime_with_sniff` | linha 3723 (`def _resolve_mime_with_sniff`) |

### `Core.Stats.CDNHealth` (95 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def record_cdn_health` | linha 6375 (`def record_cdn_health`) |
| `def snapshot_cdn_health` | linha 6409 (`def snapshot_cdn_health`) |
| `def prune_cdn_health` | linha 6424 (`def prune_cdn_health`) |
| `def _start_health_pruner` | linha 6449 (`def _start_health_pruner`) |

### `Core.Stats.Model` (66 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _Stats` | linha 6295 (`class _Stats`) |

### `Core.URL.Format` (50 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _fmt_size` | linha 4519 (`def _fmt_size`) |
| `def _fmt_host` | linha 4526 (`def _fmt_host`) |
| `def log_req` | linha 4545 (`def log_req`) |

### `Core.URL.Network` (79 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _is_ip_literal` | linha 476 (`def _is_ip_literal`) |
| `def _is_ipv6_literal` | linha 494 (`def _is_ipv6_literal`) |
| `def _is_wire_payload` | linha 513 (`def _is_wire_payload`) |
| `def _is_valid_html` | linha 521 (`def _is_valid_html`) |
| `def resolve_ip` | linha 4504 (`def resolve_ip`) |
| `def url_depth` | linha 4510 (`def url_depth`) |
| `def is_html` | linha 4513 (`def is_html`) |

### `Core.URL.Platform` (40 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def set_last_platform` | linha 548 (`def set_last_platform`) |
| `def get_last_platform` | linha 554 (`def get_last_platform`) |
| `def detect_platform` | linha 558 (`def detect_platform`) |

### `Core.URL.Policy` (29 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def is_allowed_domain` | linha 6275 (`def is_allowed_domain`) |
| `def is_external_domain` | linha 6284 (`def is_external_domain`) |

### `Core.URL.Request` (76 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _raw_path_after` | linha 445 (`def _raw_path_after`) |
| `def _request_host_header` | linha 4416 (`def _request_host_header`) |
| `def _request_hostname` | linha 4433 (`def _request_hostname`) |
| `def _proxy_base` | linha 4442 (`def _proxy_base`) |

### `Core.URL.Rewrite` (118 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def rewrite_abs_urls` | linha 4447 (`def rewrite_abs_urls`) |
| `def strip_fragment` | linha 4497 (`def strip_fragment`) |
| `def normalize_url` | linha 4500 (`def normalize_url`) |
| `def build_base_url` | linha 5601 (`def build_base_url`) |

### `Crawler.Crawl.Assets` (120 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _clean_url_match` | linha 7433 (`def _clean_url_match`) |
| `def _fetch_external_asset` | linha 7468 (`def _fetch_external_asset`) |
| `def _start_ext_asset_workers` | linha 8000 (`def _start_ext_asset_workers`) |
| `def _ext_asset_worker` | linha 8010 (`def _ext_asset_worker`) |

### `Crawler.Crawl.Hosts` (42 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _mark_host_dead` | linha 7360 (`def _mark_host_dead`) |
| `def _is_host_dead` | linha 7373 (`def _is_host_dead`) |

### `Crawler.Crawl.Jobs` (187 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def enqueue` | linha 7833 (`def enqueue`) |
| `def _crawl` | linha 7841 (`def _crawl`) |
| `def _crawl_worker` | linha 7963 (`def _crawl_worker`) |
| `def crawl_parallel` | linha 8083 (`def crawl_parallel`) |

### `Crawler.Crawl.Links` (78 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _discovery_worker` | linha 8023 (`def _discovery_worker`) |
| `def _start_discovery_workers` | linha 8042 (`def _start_discovery_workers`) |
| `def _extract_links_async` | linha 8050 (`def _extract_links_async`) |

### `Crawler.Crawl.Save` (144 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _save_worker` | linha 7519 (`def _save_worker`) |

### `Crawler.Scanner.Runner` (351 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _run_path_scanner` | linha 5936 (`def _run_path_scanner`) |

### `Crawler.Scanner.Status` (187 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _scan_paths_summary` | linha 5736 (`def _scan_paths_summary`) |
| `def _parse_status_csv` | linha 5771 (`def _parse_status_csv`) |
| `def _prompt_scan_status_filter` | linha 5796 (`def _prompt_scan_status_filter`) |

### `Crawler.Scanner.Wordlists` (83 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _load_wordlist` | linha 5670 (`def _load_wordlist`) |
| `def _resolve_wordlists` | linha 5688 (`def _resolve_wordlists`) |

### `HTTP.Headers.CI` (61 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _ci_header_keys` | linha 4009 (`def _ci_header_keys`) |
| `def _ci_get_header` | linha 4028 (`def _ci_get_header`) |
| `def _ci_remove_header` | linha 4050 (`def _ci_remove_header`) |

### `HTTP.Headers.CORS` (110 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _cors_origin_pair` | linha 3946 (`def _cors_origin_pair`) |
| `def _pna_allow_header` | linha 3969 (`def _pna_allow_header`) |
| `def _apply_corp_header` | linha 4068 (`def _apply_corp_header`) |
| `def _apply_cors_headers` | linha 4074 (`def _apply_cors_headers`) |

### `HTTP.Headers.Cookies` (132 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _all_set_cookies` | linha 3788 (`def _all_set_cookies`) |
| `def _resp_headers_dict` | linha 3833 (`def _resp_headers_dict`) |
| `def _flatten_cookiejar` | linha 3859 (`def _flatten_cookiejar`) |

### `HTTP.Headers.Forwarding` (65 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def filter_fwd` | linha 3784 (`def filter_fwd`) |
| `def inject_csrf_headers` | linha 4354 (`def inject_csrf_headers`) |
| `def rewrite_origin` | linha 4362 (`def rewrite_origin`) |

### `HTTP.Headers.Isolation` (137 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _needs_isolation` | linha 3913 (`def _needs_isolation`) |
| `def _is_worker_request` | linha 4161 (`def _is_worker_request`) |
| `def _is_document_request` | linha 4183 (`def _is_document_request`) |
| `def _coep_value` | linha 4198 (`def _coep_value`) |
| `def _apply_isolation_headers` | linha 4209 (`def _apply_isolation_headers`) |

### `HTTP.Headers.Response` (102 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def filter_resp` | linha 4257 (`def filter_resp`) |
| `def _finalize_security_headers` | linha 4331 (`def _finalize_security_headers`) |

### `HTTP.Headers.Security` (43 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _apply_security_headers` | linha 4122 (`def _apply_security_headers`) |

### `HTTP.Sessions.Clients` (63 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _get_proxy_session` | linha 3407 (`def _get_proxy_session`) |
| `def _client_id` | linha 3419 (`def _client_id`) |
| `def _get_client_session` | linha 3428 (`def _get_client_session`) |
| `def _short_exc` | linha 3451 (`def _short_exc`) |

### `HTTP.Sessions.Cloudflare` (131 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _next_cf_config` | linha 3085 (`def _next_cf_config`) |
| `def _is_cf_block` | linha 3255 (`def _is_cf_block`) |
| `def _is_raw_block_text` | linha 3343 (`def _is_raw_block_text`) |
| `def _is_bot_page` | linha 3383 (`def _is_bot_page`) |

### `HTTP.Sessions.Devices` (61 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _sanitize_ua` | linha 3016 (`def _sanitize_ua`) |
| `def _detect_device_from_ua` | linha 3019 (`def _detect_device_from_ua`) |
| `def _effective_device` | linha 3030 (`def _effective_device`) |
| `def _device_variant` | linha 3038 (`def _device_variant`) |

### `HTTP.Sessions.Factory` (154 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _make_cffi_session` | linha 3116 (`def _make_cffi_session`) |
| `def _make_session` | linha 3135 (`def _make_session`) |
| `def _make_cloudscraper_session` | linha 3195 (`def _make_cloudscraper_session`) |
| `def _make_requests_session` | linha 3229 (`def _make_requests_session`) |

### `HTTP.Streaming.Detect` (46 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _should_stream` | linha 8115 (`def _should_stream`) |
| `def _is_sse_response` | linha 9436 (`def _is_sse_response`) |

### `HTTP.Streaming.Response` (193 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _stream_resp` | linha 8139 (`def _stream_resp`) |

### `HTTP.Streaming.SSE` (176 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _sse_iter` | linha 9449 (`def _sse_iter`) |
| `def _sse_stream_generator` | linha 9490 (`def _sse_stream_generator`) |
| `def _stream_sse_resp` | linha 9527 (`def _stream_sse_resp`) |

### `Hooks.GUI.App` (1720 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _launch_hook_gui` | linha 1196 (`def _launch_hook_gui`) |

### `Hooks.GUI.Codecs` (63 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _bytes_to_hexdump` | **novo** (costura da fragmentação) |
| `def _hexdump_to_bytes` | **novo** (costura da fragmentação) |
| `def _ws_auto_encoding` | **novo** (costura da fragmentação) |

### `Hooks.GUI.Find` (35 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _find_all` | **novo** (costura da fragmentação) |

### `Hooks.Hooks.Context` (45 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class HookContext` | linha 579 (`class HookContext`) |

### `Hooks.Hooks.GUIHooks` (98 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _origin_matches` | linha 976 (`def _origin_matches`) |
| `def _apply_gui_hooks` | linha 1007 (`def _apply_gui_hooks`) |

### `Hooks.Hooks.GUITraffic` (147 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _gui_push` | linha 847 (`def _gui_push`) |
| `def _gui_push_raw` | linha 905 (`def _gui_push_raw`) |

### `Hooks.Hooks.GUIWS` (126 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _gui_ws_push` | linha 739 (`def _gui_ws_push`) |
| `def _apply_gui_ws_hooks` | linha 764 (`def _apply_gui_ws_hooks`) |

### `Hooks.Hooks.Registry` (63 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _norm_methods` | linha 609 (`def _norm_methods`) |
| `def on_request` | linha 616 (`def on_request`) |
| `def on_response` | linha 625 (`def on_response`) |
| `def _run_hooks` | linha 634 (`def _run_hooks`) |

### `Hooks.Hooks.Store` (146 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _make_hook_store` | linha 1056 (`def _make_hook_store`) |
| `def _editor_preview` | linha 1164 (`def _editor_preview`) |

### `Network.Server.Handler` (412 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _S2LWSGIRequestHandler` | linha 9805 (`class _S2LWSGIRequestHandler`) |

### `Network.Server.WSGI` (59 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _PooledWSGIServer` | linha 9757 (`class _PooledWSGIServer`) |

### `Network.Tunnels` (120 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _pump_tcp_over_ws` | linha 9632 (`def _pump_tcp_over_ws`) |

### `Proxy.Assets` (204 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def ext_asset` | linha 12277 (`def ext_asset`) |

### `Proxy.Cache.Range` (104 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _parse_range_header` | linha 10628 (`def _parse_range_header`) |
| `def _serve_range_from_disk` | linha 10768 (`def _serve_range_from_disk`) |

### `Proxy.Cache.Serve` (188 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _serve_cached` | linha 10823 (`def _serve_cached`) |
| `def _cached_response` | linha 10906 (`def _cached_response`) |

### `Proxy.Cache.Validators` (126 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _disk_validators` | linha 10659 (`def _disk_validators`) |
| `def _conditional_304` | linha 10676 (`def _conditional_304`) |
| `def _apply_freshness` | linha 10734 (`def _apply_freshness`) |

### `Proxy.Routes.Fallbacks` (162 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _cdn_fallback` | **novo** (costura da fragmentação) |
| `def _rsc_refetch` | **novo** (costura da fragmentação) |
| `def _bot_page_response` | **novo** (costura da fragmentação) |

### `Proxy.Routes.Fetch` (241 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _decide_streaming` | **novo** (costura da fragmentação) |
| `def _fetch_with_retries` | **novo** (costura da fragmentação) |
| `def _push_redirect_history` | **novo** (costura da fragmentação) |
| `def _maybe_stream` | **novo** (costura da fragmentação) |

### `Proxy.Routes.Guards` (137 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _guard_preflight` | **novo** (costura da fragmentação) |
| `def _offline_response` | **novo** (costura da fragmentação) |

### `Proxy.Routes.Main` (239 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _save_and_enqueue` | **novo** (costura da fragmentação) |
| `def _rewrite_resp_body` | **novo** (costura da fragmentação) |
| `def proxy` | linha 12499 (`def proxy`) |

### `Proxy.Routes.Recovery` (335 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _consume_upstream_body` | **novo** (costura da fragmentação) |
| `def _waf_block_retry` | **novo** (costura da fragmentação) |
| `def _handle_304` | **novo** (costura da fragmentação) |
| `def _recover_empty_body` | **novo** (costura da fragmentação) |
| `def _final_blank_page_recovery` | **novo** (costura da fragmentação) |

### `Proxy.TunnelRoutes` (27 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def ws_ext` | linha 12478 (`def ws_ext`) |
| `def tcp_ext` | linha 12486 (`def tcp_ext`) |
| `def udp_ext` | linha 12491 (`def udp_ext`) |

### `Proxy.Upstream.Headers` (88 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _main_security_headers` | linha 10212 (`def _main_security_headers`) |
| `def _normalize_request_origin` | linha 10216 (`def _normalize_request_origin`) |
| `def _cors_allow_headers` | linha 10270 (`def _cors_allow_headers`) |

### `Proxy.Upstream.Request` (261 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _upstream_url` | linha 10300 (`def _upstream_url`) |
| `def _build_ctx` | linha 10306 (`def _build_ctx`) |
| `def _is_sw_path` | linha 10321 (`def _is_sw_path`) |
| `def _do_upstream` | linha 10376 (`def _do_upstream`) |

### `Proxy.Upstream.Response` (63 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _make_flask_resp` | linha 10583 (`def _make_flask_resp`) |

### `Rewriting.HTML.Policy` (41 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _is_dont_rewrite_host` | linha 6962 (`def _is_dont_rewrite_host`) |

### `Rewriting.HTML.Reveal` (32 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _reveal_hidden` | linha 4573 (`def _reveal_hidden`) |

### `Rewriting.HTML.ServiceWorker` (59 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _inject_sw_clear` | linha 5177 (`def _inject_sw_clear`) |

### `Rewriting.HTML.URLs` (340 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _rewrite_ext_urls` | linha 6993 (`def _rewrite_ext_urls`) |

### `Rewriting.Injector.Build` (34 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _build_s2l_injector` | linha 4595 (`def _build_s2l_injector`) — lógica Python (lock, `hp`, DONT_REWRITE → `_dr_regex_js`) verbatim; o final chama `_injector_js(hp, _dr_regex_js).encode("utf-8")` |

### `Rewriting.Injector.Template` (573 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _injector_js` | **novo** (costura da fragmentação) — o miolo é o `js = ( ... )` das linhas 4618-5172 do monolito, **verbatim**; `hp`/`_dr_regex_js` eram locais do closure e viraram parâmetros |

### `Rewriting.JSON` (42 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _rewrite_json_urls` | linha 6932 (`def _rewrite_json_urls`) |

### `Runtime` (1719 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def main` | **novo** (costura da fragmentação) |

### `Viewers.CDN.Endpoint` (24 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _s2l_viewer_endpoint` | linha 11202 (`def _s2l_viewer_endpoint`) |

### `Viewers.Common` (76 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _viewer_cors_headers` | linha 11025 (`def _viewer_cors_headers`) |
| `def _viewer_disabled_response` | linha 11050 (`def _viewer_disabled_response`) |
| `def _viewer_ip_denied_response` | linha 11059 (`def _viewer_ip_denied_response`) |
| `def _viewer_ip_allowed` | linha 11065 (`def _viewer_ip_allowed`) |

### `Viewers.Data` (104 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _s2l_viewer_data_endpoint` | linha 11110 (`def _s2l_viewer_data_endpoint`) |

### `Viewers.Hosts` (26 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _s2l_hp_endpoint` | linha 10995 (`def _s2l_hp_endpoint`) |

### `Viewers.Reset` (30 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _s2l_reset_stats_endpoint` | linha 11090 (`def _s2l_reset_stats_endpoint`) |

### `WebSockets.Protocol.Compression` (67 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _ws_deflate_init` | linha 8405 (`def _ws_deflate_init`) |
| `def _ws_deflate_msg` | linha 8420 (`def _ws_deflate_msg`) |
| `def _ws_inflate_msg` | linha 8431 (`def _ws_inflate_msg`) |
| `def _ws_deflate_cleanup` | linha 8445 (`def _ws_deflate_cleanup`) |

### `WebSockets.Protocol.Frames` (190 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _ws_unmask` | linha 8369 (`def _ws_unmask`) |
| `def _ws_handshake_accept` | linha 8854 (`def _ws_handshake_accept`) |
| `def _recv_exact` | linha 8860 (`def _recv_exact`) |
| `def _ws_read_frame` | linha 8889 (`def _ws_read_frame`) |
| `def _ws_make_frame` | linha 8958 (`def _ws_make_frame`) |

### `WebSockets.Protocol.Hooks` (34 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def on_ws_message` | linha 8456 (`def on_ws_message`) |
| `def _run_ws_msg_hooks` | linha 8462 (`def _run_ws_msg_hooks`) |

### `WebSockets.Tunnel` (405 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _WSTunnel` | linha 8471 (`class _WSTunnel`) |
| `def _ws_keepalive_loop` | linha 8521 (`def _ws_keepalive_loop`) |
| `def _pump_ws_frames_v2` | linha 8573 (`def _pump_ws_frames_v2`) |

### `WebSockets.Upstream.CFFI` (164 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _CffiUpstreamWS` | linha 9249 (`class _CffiUpstreamWS`) |

### `WebSockets.Upstream.Connect` (68 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `def _get_session_for_ws` | linha 9221 (`def _get_session_for_ws`) |
| `def _ws_connect_upstream` | linha 9395 (`def _ws_connect_upstream`) |

### `WebSockets.Upstream.Raw` (245 linhas)

| Símbolo | Match no `s2l.py` |
|---|---|
| `class _RawSocketUpstreamWS` | linha 8994 (`class _RawSocketUpstreamWS`) |

---

## 5. Como continuar o trabalho de modularização

0. **Ponto de partida**: `python3 Site2Local/Boot.py` sobe o proxy modular;
   `python Site2Local/s2l.py` sobe o monolito — rode os dois para comparar
   comportamento durante o match.
1. **Escolha um alvo** no mapa acima (por tamanho: `Hooks/GUI/App.py` é o
   próximo gigante natural; `Rewriting/Injector.py` tem 588 linhas de
   template JS que caberia num `Injector/Template.py`).
2. **Abra o monolito na linha indicada** e compare seção a seção.
3. **Mova o código sem alterá-lo** — o valor do match é o diff zero.
4. **Atualize o Runtime** (import da folha) e este mapa.
5. **Rode a bateria**: `python -m compileall Site2Local`, import, e os dois
   smoke tests (OFFLINE e ONLINE) descritos na seção 2.

---

## 6. Paridade monolito ≡ modular (bateria executada)

Teste lado a lado: o monolito `s2l.py` e o pacote `Site2Local` bootados em
pastas separadas, ambos em `OFFLINE=True`, mesmo `SITE`, via *test client*
WSGI (sem sockets). **Veredito: MONOLITO ≡ MODULAR.** *(Reexecutada após a
reestruturação para pasta única — resultado idêntico.)*

| Teste | Resultado |
|---|---|
| **Símbolos (AST)** | 193 defs/classes de topo no monolito → **0 perdidos** na árvore modular. Os 24 extras são as 21 costuras da seção 2 + `create_app` (`Application.py`) + `main` (`Runtime.py`) + `_coep_credentialless_supported` (correção pós-match nº 3a). |
| **Corpos (match nível símbolo)** | **186/193 corpos idênticos** ao monolito na comparação AST pós-normalização (docstrings novas, decorators de rota, aliases `Runtime` e a troca `global`→atributo-do-hub dissolvidos — ver regras abaixo). Os 4 restantes são as costuras documentadas (`proxy`, `_launch_hook_gui`, `_s2l_viewer_endpoint`, `_build_s2l_injector`), cuja fidelidade é provada por comportamento (hashes dos smoke + injetor byte-idêntico) e pelos fragmentos verbatim. Os 3 que divergem **de propósito** são correções pós-match: `_do_upstream` (nº 2) e `_coep_value` + `_apply_isolation_headers` (nº 3a — ver subseção abaixo). |
| **Constantes de módulo (match nível estado)** | 196 constantes de topo no monolito → **194/194 estáveis idênticas** no Runtime (0 ausentes, 0 diferentes). As 2 restantes (`_START_TS`, `_last_platform`) flutuam entre dois boots do próprio monolito e estão presentes no Runtime. |
| **Config** | 27 knobs (`SITE`…`SCAN_PATHS`) idênticos nos dois lados. |
| **Rotas Flask** | 10/10 regras idênticas: mesma URL, mesmos métodos, mesmo endpoint. |
| **Smoke OFFLINE** | 9 endpoints com mesmo status, mesmo `Content-Length` e mesmo `sha256` do corpo — inclusive o viewer de 63.533 bytes, byte-idêntico. |
| **Injetor `__s2l__`** | `_build_s2l_injector()` byte-idêntico no fechamento do match: 11.604 bytes nos dois lados. Hoje 11.815 no modular — a correção pós-match nº 3b acrescentou o patch de `setProperty` (o monolito segue em 11.604, congelado como referência). |

### Regras de normalização do match de corpos

A comparação AST dissolve (nos DOIS lados) apenas diferenças que a própria
modularização exige — qualquer outra diferença contaria como divergência:

| Normalização | Por quê é legítima |
|---|---|
| docstring removida | costuras ganharam docstrings novas ("Extracted from…") |
| decorators removidos | no modular as rotas são registradas via `add_url_rule` no `Application.py` (paridade de `url_map` provada à parte) |
| `ALIAS.Nome` → `Nome` (quando ALIAS importa o Runtime) | o monolito usa o nome direto; as folhas usam `_s2l_state.Nome` |
| aliases de import resolvidos (ex.: `flask_request` → `request`) | mesmo objeto, nome local diferente |
| `global X` removido | no modular o estado mutável de módulo virou atributo do hub (`_s2l_state.X`) — transformação semanticamente obrigatória |

Com essas 5 regras, **187 dos 193 corpos colapsam em idênticos** (antes das correções pós-match nº 2 e nº 3a; depois delas, 186) — ou seja,
a lógica foi migrada sem alterações; o que mudou é só o endereçamento do
estado, exatamente como a arquitetura pede.

### Quirk replicado de propósito (compatibilidade bug-a-bug)

No monolito, as três rotas de túnel registram a regra com um nome e a view
assina com outro (`<path:wspath>` ↔ `def ws_ext(_wspath)`, linhas
12477-12493 do `s2l.py`). O Flask passa `wspath=…` como *keyword* para uma
função que só aceita `_wspath` → **TypeError → 500** em vez do 426 didático
que o comentário do próprio código descreve. Impacto mínimo: essas views só
rodam quando alguém abre a URL do túnel direto no navegador (o tráfego WS
real é interceptado na camada de socket, antes do Flask).

A costura modular havia alinhado os nomes (regra `_wspath` ↔ view `_wspath`
→ 426). **Revertido ao comportamento do monolito** durante a fase de match:
as regras em `Application.py` voltam a ser `wspath`/`tcppath`/`udppath`
verbatim. Para consertar de verdade quando sair do modo match, é uma linha
por rota: alinhar o nome da variável da regra com a assinatura da view.

### Divergência `SITE` (preexistente do projeto, não da costura)

O monolito embute `SITE = "example.com"` (linha 272), mas o `Config.py`
histórico do projeto já era `SITE = "jeskojets.com"`. O modular preserva o
`Config.py` do usuário (o knob certo). Na bateria de paridade o `SITE` foi
equalizado nos sandboxes de teste para isolar a variável — os 2 bytes de
diferença que aparecem no `viewer_data` são só o campo `"target"`.

### Correções pós-match (fase 2 — divergências funcionais deliberadas)

Os testes de campo reais (usuário navegando um portal de jogos com detecção
server-side de dispositivo e iframes de jogo cross-origin, `poki.com`)
expuseram três buracos que o monólito também tem — **não são regressões da
costura**, são comportamentos originais que a fase de match preservou
fielmente. Corrigidos **só no pacote modular**; o monólito `s2l.py` segue
congelado como referência do match. Cada conserto é um bloco pequeno e
localizado se quiser retroportar.

**Sintoma relatado (correções 1-2)**: com `DEVICE` em `auto`, `desktop` ou
`macintosh`, o site entregava o HTML da versão de celular
(`<body class="mobileVersion">`), idêntico nos três perfis.

**Diagnóstico de campo (reproduzido)**: o site-alvo decide a variante
**exclusivamente pelo User-Agent** — cookies de A/B e client hints não
influenciam (testado um a um); todos os UAs desktop-class (Windows, Mac,
Linux, ChromeOS, Safari, IE11, Googlebot e até ausência de UA) recebem
`desktopVersion`. O caminho vivo do proxy estava correto (verificado
ponta a ponta: `auto`+desktop → desktop, `auto`+mobile → mobile em
variante própria, cache hit preservando o canonical). O que restou foi o
**cache canonical servindo HTML mobile gravado anteriormente** (perfil
antigo ou build pré-variante): `auto`/`desktop`/`macintosh` mapeiam todos
para o MESMO arquivo canonical, e nada jamais revalidava esse arquivo ao
trocar o perfil.

**Sintoma relatado (correção 3)**: no Firefox, o DevTools (Rede) mostra
duas requests a `a.poki-cdn.com/.../rotation.svg` com
`NS_ERROR_DOM_CORP_FAILED` (0 B, iniciador `img`) ao abrir um jogo — o
recurso é bloqueado pela política CORP da página.

**Diagnóstico de campo (reproduzido)**: o monólito (e o modular, até esta
correção) aplica COOP `same-origin` + COEP em **toda** navegação de
documento (`_is_document_request()`). No Chromium o COEP é
`credentialless`, que carrega subrecursos cross-origin anônimos sem CORP —
mas **Firefox e Safari não implementam `credentialless`** (MDN, dez/2025):
para eles resta `require-corp`, que **bloqueia duro** qualquer subrecurso
cross-origin no-CORS cuja resposta não traga CORP. Ora, o proxy reescreve
e proxifica a esmagadora maioria das URLs, mas uma URL construída em
runtime pode escapar de todos os patches (template literals como
`https://cdn/icons/${nome}.svg` no SDK do Poki, custom properties CSS com
`url()` — `--icon-src`, imgs injetadas via innerHTML antes do microtask do
MutationObserver). No Chrome esses fugitivos carregam; no Firefox morrem
com `NS_ERROR_DOM_CORP_FAILED`. O site real, vale notar, **não envia
COOP/COEP/CORP nenhum** — o blanket era uma restrição adicionada pelo
proxy, não pelo site espelhado. (Contexto completo: cadeia de realms
poki.com → games.poki.com (wrapper) → gdn.poki.com (jogo), todos servidos
por portas CDN locais com injetor; os chunks do jogo carregam de
`localhost:8081`; só os URLs construídos em runtime pelo SDK escapam.)

| # | Correção | Onde | Comportamento novo |
|---|---|---|---|
| 1 | Revalidação de cache ao trocar `DEVICE` | `Runtime.py` (`_DEVICE_CHANGED`, `_DEVICE_REFRESHED` + `_note_device_change()` aninhado em `main()`) e `Proxy/Routes/Main.py` (Fase 3 do `proxy()`) | O perfil ativo é gravado em `site_data/<site>/device_profile.json` a cada boot. Quando muda entre boots: WARN no log + todo documento HTML em cache é re-buscado upstream **uma única vez** (por arquivo, por sessão) e regravado com a variante do perfil atual — assets continuam vindo do cache (perfis desktop-class compartilham os mesmos). Cura mirrors cujo canonical foi gravado sob outro perfil/build. |
| 2 | Client hints coerentes com o perfil | `Proxy/Upstream/Request.py` (`_do_upstream`) | Perfis explícitos (`DEVICE != "auto"`) removem os `Sec-CH-UA*` do navegador real e instalam os do perfil (`_SEC_CH_UA`); `auto` continua espelhando o navegador (backfill incluso), e os `Sec-Fetch-*` passam intactos nos dois modos. Motivo: quem testa em emulação mobile (DevTools) mandava `Sec-CH-UA-Mobile: ?1` junto com o UA desktop do perfil — backends hint-first serviriam mobile mesmo com `DEVICE=macintosh`. |
| 3a | Isolamento COOP/COEP consciente do navegador | `HTTP/Headers/Isolation.py` (`_coep_credentialless_supported()` novo + gate em `_apply_isolation_headers`) | O blanket de COOP+COEP em navegação de documento passa a ser **exclusivo de Chromium** (única família com `credentialless`). Firefox/Safari só recebem isolamento quando a própria página pede (heurística `_needs_isolation()` — marcadores SAB/WASM no corpo) ou quando é script de Worker (caminho inalterado, `require-corp`). Espelha a postura do site real (a maioria não envia COEP) e elimina o `NS_ERROR_DOM_CORP_FAILED` do Firefox: os fugitivos passam a carregar direto do CDN real (online) em vez de serem bloqueados. `_coep_value()` foi refeito em cima do helper novo — mesma regex, mesmo resultado. |
| 3b | Patch de `setProperty` no injetor | `Rewriting/Injector/Template.py` (bloco CSS, após o patch de `cssText`) | `CSSStyleDeclaration.prototype.setProperty` agora passa valores com `url(...)` pelo `_cssRw` — cobre custom properties CSS (`--icon-src: url(https://cdn/x.svg)`, o sistema de ícones do Poki usa exatamente isso via props de estilo do React), que os setters `backgroundImage`/`cssText` nunca viam. Essas URLs agora são reescritas pelo `rw()` e carregam pela porta CDN local em vez de irem direto ao CDN real. Injetor: 11.604 → 11.815 bytes. |
| 4 | Fim do `localhost:-1` (sentinela de bind do MULTIPORT) | `Runtime.py` (loop do resumo do `_bg_crawl`), `Rewriting/Injector/Build.py` (filtro `p > 0` no bake inicial do `HP`) e `Rewriting/Injector/Template.py` (poll do HP espelha o mapa do servidor) | **Sintoma**: no boot do mirror `jeskojets.com` o resumo pós-crawl imprimiu `CDN cdn.prod.website-files.com → http://localhost:-1`. **Diagnóstico**: `-1` é o sentinela "mini-server ainda fazendo bind" (`CDN/Registry/Hosts.py`) — a porta real só entra no mapa quando o `make_server` do werkzeug conclui (`CDN/Server.py`), e a linha `host → :8081` é logada só DEPOIS disso; o resumo do crawl tirava o snapshot no instante em que `crawl_parallel()` retornava (crawl de 2 páginas terminou no mesmo segundo do registro) e imprimia a cópia velha. O mapa em si nunca esteve errado — nada roteou para a porta -1. **Correção**: o resumo agora relê a porta viva na hora de imprimir, com carência limitada (≤2s em passos de 50ms) para hosts ainda em bind, pula entradas que sumiram (bind falhou — ERROR já logado pelo `_start_cdn_server`) e rotula `0` como rota `/__s2l_ext__/`. **Auditoria de todos os consumidores do mapa**: o rewriter server-side (`_proxy_target`) já protegia `port > 0`; `/__s2l_hp`, viewer e health já filtravam `p > 0`; `Guards.py`/`Fallbacks.py` só usavam as chaves. O único vazamento real era o **bake inicial do injetor**, que não filtrava: página servida na janela de milissegundos entre sentinela e bind levava `HP[host] = -1` (truthy em JS → `rw()` emitia `http://localhost:-1/...`), e o poll add-only (`if(!HP[h])`) nunca reparava um valor existente. O bake agora filtra `p > 0` (mesmo filtro do `/__s2l_hp`) — nessa janela o host cai na rota `/__s2l_ext__/` da porta principal até o poll (1º tick 1,5s) trazer a porta real — e o poll passou a **espelhar** o mapa: adiciona host novo, atualiza porta mudada (crash + re-register pega porta NOVA; o add-only manteria a morta para sempre) e remove host cujo servidor morreu (URLs voltam para a rota `/__s2l_ext__/`, que é o que re-registra o host). Injetor: +30 bytes. |
| 5 | Resgate de MIME por conteúdo (scripts atrás de extensão exótica) | `Core/MIME/Guess.py` (`_looks_like_js` + `_js_mime_rescue` novos), aplicado em `Proxy/Cache/Serve.py` (`_serve_cached` + probe 304 do `_cached_response`), `CDN/Server.py` (probe 304 + caminho vivo), `Proxy/Assets.py` (probe 304 + caminho vivo), `Proxy/Cache/Range.py` (206), `Proxy/Routes/Fallbacks.py` (fallback de disco + vivo) e `Proxy/Cache/Validators.py` (`_disk_validators` agora dobra o CT no ETag); costura no `Runtime.py` | **Sintoma**: no mirror `jeskojets.com` a seção Global renderizava uma **bola preta** no lugar do globo 3D texturizado com arcos (comparação lado a lado com o site real). **Diagnóstico (reproduzido com Chromium headless)**: a página carrega `<script src="https://cdn.jsdelivr.net/npm/globe.gl">` — o NOME do pacote termina em `.gl`, e a stdlib mapeia `.gl` → `video/gl` (formato Graphics Language de 1993, ainda no registro IANA). O cache em disco guarda só o corpo (o `application/javascript` do upstream não é persistido), então todo serve de cache-hit re-deriva o Content-Type da extensão e entrega `video/gl`. Com `X-Content-Type-Options: nosniff` (nós e o jsdelivr mandamos), o Chromium **se recusa a executar** script clássico com MIME não-JS: `window.Globe` nunca existe, o guard `typeof Globe !== 'function'` do chunk da Slater retorna cedo, e o container estilizado (círculo escuro) aparece no lugar do WebGL. A textura (`globe-map.svg`) nem chegava a ser pedida. Primeira visita (fetch vivo, CT do upstream) funcionava; visitas seguintes (cache-hit) quebravam — o clássico "funcionou uma vez, depois nunca mais". **Correção**: `_js_mime_rescue(ct, lp, head)` — quando o tipo derivado NÃO é JavaScript e NÃO está na lista de nunca-tocar (html/css/xml/json/svg/wasm, onde um head parecido com JS seria falso positivo — banners `/*!` de CSS são quase idênticos aos de JS), cheia os primeiros 512 bytes com `_looks_like_js` (strippa até 3 blocos de comentário líder e casa prefixos de token de alta precisão; prosa inglesa como "various"/"constant" não casa por exigir espaço depois do token) e serve como `text/javascript; charset=utf-8`. Bytes idênticos, rótulo certo; binários reais (PNG/MP4/WASM) nunca casam e mantêm o tipo da extensão. Aplicado em TODOS os pontos que derivam CT de disco (cache-hit 200/304/206 do host principal, mini-server CDN, rota `/__s2l_ext__/`, fallbacks) e também nos caminhos vivos (CDN + ext + fallback), cobrindo CT de upstream ausente/mentiroso. **Cura de caches envenenados**: o browser guarda o Content-Type da resposta ARMAZENADA através de 304s casados — quem já cacheou `video/gl` nunca veria o tipo corrigido; o ETag agora dobra um crc32 do CT base (`s2l:size:mtime:xxxxxxxx`), então a mudança de derivação invalida o validator e força um 200 fresco por asset por cliente (e qualquer correção futura de MIME também auto-curará). Log WARN deduplicado (cap 256) por arquivo resgatado. |

Verificação (1-2): `device_fix_verify.sh` (scripts do projeto) — **19/19
verde**: cenário completo de envenenamento do canonical → troca de perfil
→ WARN → re-fetch único → cache hit na 2ª visita; coerência de hints
provada com servidor-eco upstream; trio de regressão do modo `auto` sem
alteração.

Verificação (3): `corp_fix_verify.sh` (scripts do projeto) — **15/15
verde**: Firefox + documento + página comum → **sem** COEP/COOP (CORP
`cross-origin` permanece como salvaguarda universal); Chrome + documento →
COOP `same-origin` + COEP `credentialless` (blanket mantido); Firefox +
request de Worker → COEP `require-corp` (inalterado); Firefox + página com
marcadores SAB → isolada mesmo sem blanket (caminho da heurística); UA
não-Chromium → sem COEP; injetor servido na página carrega o patch de
`setProperty` e o `<script id="__s2l__">` extraído passa no `node --check`;
endpoints de regressão (viewer/hp/reset) 200. Ao vivo contra `poki.com`:
`/br/g/subway-surfers` com UA Firefox sai sem COOP/COEP e com UA Chrome sai
`credentialless` — e a bateria de paridade completa (config 27/27, rotas
10/10, smoke 9/9 com hashes idênticos) continua verde após a correção.

Verificação (4): `fix_port_minus1_verify.py` (scripts do projeto) — **18/18
verde**: `py_compile`/`compileall` do pacote; bake do injetor com mapa falso
`{-1, 0, 8085}` sai só com a porta viva e sem `localhost:-1`/`localhost:0`;
injetor extraído passa no `node --check` (corpo de 11.835 chars); injetor
completo: 11.865 bytes (+30 sobre a expressão add-only do 3b, mesma base de
medição); semântica do poll
espelho comprovada em Node (adiciona `fresh`, atualiza `stale`, remove
`dead`); loop do resumo simulado nos cinco estados — ligado (→ porta real),
liga-durante-a-carência (→ porta real após ~120ms), bind-falhou (→ nenhuma
linha), sem porta dedicada (→ rota `/__s2l_ext__/`), nunca-liga (→ "port
pending") — e `localhost:-1` não aparece em nenhum caminho.

Verificação (5): `fix_mime_rescue_verify.py` (scripts do projeto) — **46/46
verde**, contra o proxy vivo espelhando `jeskojets.com` (crawl real + cache
em disco): `compileall` do pacote; bateria de `_looks_like_js` com heads REAIS
do mirror (globe.gl `// banner → !function`, lenis `function`, banner `/*!` do
gsap) e falsos-positivos fechados (CSS com banner `/*!`, JSON objeto/array,
HTML, PNG, WASM, prosa "various/constant", banner não-terminado);
bateria do resgate (video/gl+JS→JS, video/gl+binário→intacto, octet-stream+
JS→JS, text/plain+JS→JS, text/plain+prosa→intacto, nunca-tocar em
css/html/json/svg/wasm/manifest+json/+xml/+json, JS→JS inalterado, head
vazio/arquivo inexistente→intacto); ETag (formato 3 segmentos, estável por
CT, difere entre CTs, difere do legado sem CT); ao vivo — `npm/globe.gl`
servido `text/javascript; charset=utf-8` com WARN "MIME rescue" deduplicado
no log, 304 com ETag consistente, **ETag legado sem CT não casa mais → 200
fresco** (cura cache envenenado), e zero regressão (`gsap.min.js`
text/javascript, `lenis.css` text/css, `img_jet.webp` image/webp; HTML servido
reescreve globe.gl para `:8083` e injeta `__s2l__`). Prova de ponta a ponta
em Chromium headless contra o proxy: antes do fix `window.Globe ===
'undefined'` e nenhum canvas em `[globe-container]`; depois `typeof Globe ===
'function'`, canvas 587×587 com textura carregada, e a captura da seção
#global confirmada por VLM: globo 3D com continentes visíveis + arcos
tracejados de rota — paridade visual com o site real. **Re-verificação E2E
(08/11/2026, `scripts/globe_e2e_verify.sh` + `globe_luminance.sh`)**: boot
fresco com cache em disco → `:8083/npm/globe.gl` cache-hit `text/javascript;
charset=utf-8` (ETag `s2l:size:mtime:crc32ct`), 304 consistente, HTML servido
reescreve o script para `:8083`, `typeof Globe === 'function'` no mirror, 1
canvas, zero erros de página; máscara/overlay/textura (`globe-mask.webp`,
`globe-over.webp`, `globe-map.svg`) todos 200 pelas portas CDN; mesmo
`scrollY`/DOM/canvas 528×528 que o site ao vivo; e **luminância pixel-a-pixel
do canvas idêntica** (5 frames de cada lado, média 36.0 × 36.0, evolução
temporal igual) — diferenças residuais de brilho em capturas avulsas são
fase da animação de entrada, não defeito do mirror. Comparativo salvo em
`download/globe_mirror_vs_live.jpg`.

### Princípio da universalidade (auditoria 08/11/2026)

> **Nenhuma correção pode ser amarrada a um site.** Site-alvo é INPUT
> (`Config.SITE`), não lógica. Um bug achado num site real (poki, jeskojets)
> é o *detector* da classe do problema — a correção ataca a classe, nunca o
> caso. Toda a corrente de decisões do proxy é relativa: "externo" = não é
> `MAIN_HOST`; "CDN" = qualquer host que serviu asset com sucesso (registro
> dinâmico, mapa em runtime); MIME = cheirado do conteúdo; navegador =
> família de engine (Gecko/Chromium); dispositivo = classe de UA.

**Auditoria de especificidade (pacote modular, s2l.py congelado à parte)**:
varredura por nomes de sites/CDNs e por literais de domínio em todo o `.py`
do pacote. Resultado — o único estado site-específico do projeto é o input
do usuário e o dado derivado dele:

| O que existe no código | Classificação | Por que é universal |
|---|---|---|
| `Config.SITE` | **input** | o alvo escolhido pelo usuário — é *suposto* ser específico |
| docstrings citando poki/jeskojets/jsdelivr | **documentação** | história dos bugs de campo; compilador ignora |
| `_BOT_PAGE_SIGNATURES` (Google Sorry, DDoS-Guard, hCaptcha, reCAPTCHA) e `_is_cf_block` (cf-mitigated, jschl_vc) | **tecnologia** | fingerprints de páginas de desafio anti-bot — qualquer site atrás desses WAFs se beneficia; detectam marcas no CORPO da resposta, não roteiam por domínio |
| `_coep_credentialless_supported()` | **tecnologia** | família de engine (Gecko vs Chromium), não site |
| `Devices.py` (perfis de UA) | **tecnologia** | classes de dispositivo, não sites |
| heurísticas de caminho (`/api/`, `/assets/`, `/builds/`) | **convenção** | padrões de path universais da web; errar muda só a forma de fetch, não o resultado |
| `_js_mime_rescue` | **conteúdo** | pergunta única: "esses bytes parecem JavaScript?" — zero referência a host/URL no código executável (verificado por AST) |

Linha que NÃO existe em lugar nenhum: `if host == "site.tld"`. A lista
`CDN_BLOCK` (blocklist de hosts) é **vazia por default** — é config do
usuário, não hardcode. As seis correções pós-match seguem todas o princípio:
1-2 perfil de dispositivo (classe de UA), 3a engine de navegador, 3b API CSS
padrão, 4 ciclo de vida de porta (arquitetura interna), 5 conteúdo do corpo.

### Auditoria final de completude da migração (08/11/2026, pós-correção 5)

Re-staging fresco de ambas as árvores (`/tmp/parity`, `SITE=example.com`,
cache vazio determinístico) e bateria completa re-executada contra o estado
ATUAL do código: **config 27/27 idênticos · rotas 10/10 idênticas (regra +
métodos + endpoint) · smoke offline 9/9 com sha256 idênticos** (`parity_driver`
×2 + `parity_compare`). **Símbolos**: 193 defs/classes de topo do monólito →
**0 perdidos**; 26 extras = as 21 costuras documentadas na seção 2 + os
helpers das correções (`_coep_credentialless_supported` 3a, `_looks_like_js`/
`_js_mime_rescue` 5) + `create_app`/`main`/`_injector_js` do próprio layout
modular. **Constantes** (`parity_constants.py` — lacuna fechada: a bateria
original só olhava def/class): 196 assignments de módulo do monólito →
**0 perdidos**; 10 extras = constantes das correções 1 (`_DEVICE_*`) e 5
(`_JS_*`, `_NEVER_JS_RESCUE`, `_js_rescue_logged`) + `_CDN_VIEWER_PAGE`
(template extraído) + `_HERE`/`_PARENT` do Boot + alias `config`. **Injetor**
(`injector_diff_verify.py`): +241 bytes sobre o monólito = exatamente +211
(fix 3b, patch de `setProperty`) e +30 (fix 4, poll do HP com add/update/
delete) — **zero adições fora dos fixes documentados**. **Pontos de entrada**
(`entry_points_verify.sh`): `python -m Site2Local`, `python3 Site2Local/
Boot.py` e `python3 s2l.py` sobem e escutam; `import s2l` histórico resolve.
Monólito confirmado congelado (única diferença vs staging: comentário na
linha do `SITE`). **Veredito: migração completa — nada pendente; as únicas
divergências monólito→modular são as 6 correções pós-match deliberadas.**
