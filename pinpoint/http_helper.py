# pinpoint-python-agent
# Copyright (c) 2026-present NAVER Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""HTTP request/response trace helpers.

Everything here runs interpreter-side: metadata (remote address, endpoint,
status, url_stat, identity annotations) is buffered on the Python wrappers and
flushed in the one native finalize call at ``end()``. Header/cookie recording
resolves the configured allow-list (or ``HEADERS-ALL``) and extracts the header
values into buffered two-string annotations, so native never iterates a Python
reader. ``Pinpoint-Proxy*`` monitoring headers, and configured user proxy
headers, are parsed here too and buffered as one composite annotation each.

Cookie recording takes a *cookie reader* whose entries are individual cookies
(name/value pairs from the parsed ``Cookie`` header);
``Http.{Server,Client}.RecordRequestCookie`` then filters which cookies
actually land on the span.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping, Optional, Tuple, Union
import re
from urllib.parse import parse_qsl

from .agent import _INT32_BOUND, _SNAPSHOT_SQL_TRACE_BIND_VALUE, get_agent
from .context import current_span
from .propagator import PINPOINT_HEADERS
from .annotation import (
    ANNOTATION_HTTP_COOKIE,
    ANNOTATION_HTTP_PROXY_HEADER,
    ANNOTATION_HTTP_REQUEST_HEADER,
    ANNOTATION_HTTP_RESPONSE_HEADER,
    ANNOTATION_HTTP_STATUS_CODE,
    ANNOTATION_HTTP_PARAM,
    ANNOTATION_HTTP_URL,
)
from .service_type import SERVICE_TYPE_PYTHON_HTTP_CLIENT


HeadersLike = Union[
    Mapping[str, Any],
    Iterable[Tuple[Any, Any]],
    "HeadersReader",
    "ASGIHeadersReader",
    None,
]


def _headers_items_lower(headers: HeadersLike) -> Tuple[list, dict]:
    """Stringify a header mapping/iterable into ``(items, lower)``.

    ``items`` preserves original-case ``(key, value)`` pairs (for ``ForEach``);
    ``lower`` maps lowercased key → value (for case-insensitive ``get``). Last
    value wins on case-collision — matches how a normal request handler sees
    combined-or-last header values. ``None`` keys/values are skipped.
    """
    items: list = []
    lower: dict = {}
    if headers is None:
        return items, lower
    if hasattr(headers, "items") and callable(headers.items):  # type: ignore[union-attr]
        iterator: Iterable[Tuple[Any, Any]] = headers.items()  # type: ignore[union-attr]
    else:
        iterator = headers  # type: ignore[assignment]
    for k, v in iterator:
        if k is None or v is None:
            continue
        ks = str(k)
        vs = str(v)
        items.append((ks, vs))
        lower[ks.lower()] = vs
    return items, lower


class HeaderReader:
    """Base of the header readers below: a case-insensitive ``get(name)`` plus
    ``for_each(callback)`` iteration where the callback returning ``False``
    stops. Native code never consumes a reader; span creation takes the
    pre-extracted Pinpoint-header dict instead."""

    __slots__ = ()


class DictHeaderReader(HeaderReader):
    """Reader over a pre-lowercased header dict.

    ``lower`` maps lowercased-name -> value and drives ``get``; ``items``
    holds original-case pairs for ``for_each`` (HEADERS-ALL recording) and may
    be ``None`` for get-only readers that never iterate.
    """

    __slots__ = ("_lower", "_items")

    def __init__(self, lower: Mapping[str, str], items=None):
        self._lower = lower
        self._items = items

    def get(self, key: str) -> Optional[str]:
        return self._lower.get(str(key).lower())

    def for_each(self, callback: Callable[[str, str], bool]) -> None:
        for k, v in self._items or ():
            if callback(k, v) is False:
                return


def HeadersReader(headers: HeadersLike = None) -> HeaderReader:
    """Build a dict-backed :class:`HeaderReader` from headers.

    Despite the class-style name (kept for call-site compatibility) this is a
    thin factory: it stringifies the mapping/iterable once and hands the
    resulting lowercased dict + original-case items to
    :class:`DictHeaderReader`, so every subsequent ``get`` is one dict lookup.

    An already-built reader is returned unchanged so callers can pass one
    through without re-parsing.
    """
    if isinstance(headers, HeaderReader):
        return headers
    items, lower = _headers_items_lower(headers)
    return DictHeaderReader(lower, items)


class EnvironHeaderReader(HeaderReader):
    """Lazy HeaderReader over a raw WSGI ``environ`` / Django ``request.META``.

    Instead of eagerly flattening the whole environ (typically 40-60 keys, each
    ``startswith`` + ``replace`` + ``str``) into a dict, ``get`` maps a single
    header name to its ``HTTP_*`` environ key and does one dict lookup. The full
    header list is walked only when ``for_each`` (HEADERS-ALL recording)
    asks for it.

    This keeps the "request carries upstream Pinpoint context" path cheap even
    when the request is *unsampled*: only the ten propagation keys (plus a
    handful of proxy/XFF keys when sampled) are ever touched, versus building
    the entire header dict up front.
    """

    __slots__ = ("_environ",)

    def __init__(self, environ: Mapping[str, Any]):
        self._environ = environ

    def get(self, key: str) -> Optional[str]:
        ku = str(key).upper()
        if ku == "CONTENT-TYPE":
            environ_key = "CONTENT_TYPE"
        elif ku == "CONTENT-LENGTH":
            environ_key = "CONTENT_LENGTH"
        else:
            environ_key = "HTTP_" + ku.replace("-", "_")
        value = self._environ.get(environ_key)
        if value is None:
            return None
        return str(value)

    def for_each(self, callback: Callable[[str, str], bool]) -> None:
        # PEP 3333 ``HTTP_*`` vars plus the two unprefixed content vars.
        for key, value in self._environ.items():
            if not isinstance(key, str):
                continue
            if key.startswith("HTTP_"):
                name = key[5:]
            elif key in ("CONTENT_TYPE", "CONTENT_LENGTH"):
                name = key
            else:
                continue
            try:
                if callback(name.replace("_", "-"),
                            "" if value is None else str(value)) is False:
                    return
            except Exception:  # noqa: BLE001
                return


class MultiDictHeaderReader(HeaderReader):
    """Lazy HeaderReader over a case-insensitive header multimap.

    Wraps tornado ``HTTPHeaders`` / aiohttp ``CIMultiDict`` (both look up
    case-insensitively) so ``get`` delegates straight to the container — only
    the requested keys are touched, avoiding an up-front flatten of every header
    on requests that carry upstream context. ``for_each`` materializes the flat
    list lazily for HEADERS-ALL recording.
    """

    __slots__ = ("_raw",)

    def __init__(self, raw: Any):
        self._raw = raw

    def get(self, key: str) -> Optional[str]:
        try:
            value = self._raw.get(str(key))
        except Exception:  # noqa: BLE001
            return None
        if value is None:
            return None
        return str(value)

    def for_each(self, callback: Callable[[str, str], bool]) -> None:
        raw = self._raw
        try:
            pairs = raw.get_all() if hasattr(raw, "get_all") else raw.items()
        except Exception:  # noqa: BLE001
            return
        for k, v in pairs:
            try:
                if callback(str(k), str(v)) is False:
                    return
            except Exception:  # noqa: BLE001
                return


def _decode_header_part(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("latin-1")
    return str(value)


# After this many raw reverse-scan lookups, ASGIHeadersReader decodes its header
# list once and serves the rest from an O(1) dict. The usual unsampled two-probe
# peek stays on the cheap raw path; a sampled request drives enough lookups to pay
# the decode off. Measured 2026-08-20 (M1 Pro, CPython 3.14): raw keeps the
# unsampled peek flat at ~1.96us at any header count, while decode-first scales
# to 4.9/8.5/14.4us at 8/20/40 headers. Decode-first wins ~2us back on the
# sampled 10-probe path, which is noise next to the ~13us the span itself costs.
_ASGI_GET_DECODE_THRESHOLD = 2


class ASGIHeadersReader(HeaderReader):
    """Adapts ASGI ``scope['headers']`` byte pairs lazily.

    The common ASGI server path is unsampled, so most requests only need a few
    trace-context lookups during span creation. Keep the raw byte pairs intact
    and decode the full header list only when HEADERS-ALL recording asks to
    iterate all entries.
    """

    __slots__ = ("_raw", "_items", "_lower", "_get_count")

    def __init__(self, headers: Iterable[Tuple[Any, Any]] = ()):
        # ``scope["headers"]`` is already a list/tuple of (bytes, bytes) pairs, so
        # keep the reference; other iterables are materialized once so the raw
        # pairs survive re-scanning across ``get`` calls.
        if not headers:
            self._raw: Iterable[Tuple[Any, Any]] = ()
        elif isinstance(headers, (list, tuple)):
            self._raw = headers
        else:
            self._raw = tuple(headers)
        self._items: Optional[list] = None
        self._lower: Optional[dict] = None
        self._get_count = 0

    def get(self, key: str) -> Optional[str]:
        lower_key = str(key).lower()
        if self._lower is not None:
            return self._lower.get(lower_key)

        # A raw lookup is an O(n) reverse scan plus a latin-1 encode of the probe
        # key — cheaper than decoding everything for the first couple of probes,
        # but a sampled request drives ~10 lookups, at which point the O(1)
        # ``_lower`` dict wins. Flip over past the threshold.
        self._get_count += 1
        if self._get_count > _ASGI_GET_DECODE_THRESHOLD:
            self._decoded_items()
            return self._lower.get(lower_key)  # type: ignore[union-attr]

        # Reverse iteration with an early return gives HeadersReader's
        # last-occurrence-wins semantics without scanning every header.
        key_bytes = _latin1_lower_bytes(lower_key)
        for k, v in reversed(self._raw):
            if k is None or v is None:
                continue
            if not _header_name_matches(k, lower_key, key_bytes):
                continue
            try:
                return _decode_header_part(v)
            except Exception:  # noqa: BLE001
                continue
        return None

    def for_each(self, callback: Callable[[str, str], bool]) -> None:
        for k, v in self._decoded_items():
            try:
                if callback(k, v) is False:
                    return
            except Exception:  # noqa: BLE001
                return

    def _decoded_items(self) -> list:
        items = self._items
        if items is not None:
            return items

        items = []
        lower = {}
        for k, v in self._raw:
            if k is None or v is None:
                continue
            try:
                ks = _decode_header_part(k)
                vs = _decode_header_part(v)
            except Exception:  # noqa: BLE001
                continue
            items.append((ks, vs))
            lower[ks.lower()] = vs
        self._items = items
        self._lower = lower
        return items


def _latin1_lower_bytes(value: str) -> Optional[bytes]:
    try:
        return value.encode("latin-1")
    except UnicodeEncodeError:
        return None


def _header_name_matches(raw_key: Any, lower_key: str,
                         key_bytes: Optional[bytes]) -> bool:
    if isinstance(raw_key, bytes):
        return (
            key_bytes is not None
            and (raw_key == key_bytes or raw_key.lower() == key_bytes)
        )
    if isinstance(raw_key, bytearray):
        raw = bytes(raw_key)
        return (
            key_bytes is not None
            and (raw == key_bytes or raw.lower() == key_bytes)
        )
    try:
        return str(raw_key).lower() == lower_key
    except Exception:  # noqa: BLE001
        return False


# WSGI-normalized forms of every Pinpoint propagation header, so dict membership
# replaces a scan of the whole 40+-key environ on the common no-upstream-context path.
_ENVIRON_PINPOINT_KEYS = frozenset(
    "HTTP_" + name.upper().replace("-", "_") for name in PINPOINT_HEADERS
)


def has_pinpoint_environ(environ: Mapping[str, Any]) -> bool:
    """True if a WSGI ``environ`` (or Django ``request.META``) carries an
    upstream Pinpoint trace-context header.

    PEP 3333 requires ``environ`` to be a builtin dict, so the fast path is
    a handful of O(1) membership checks against the known header set; the
    key-prefix scan remains as a fallback for exotic mappings.
    """
    try:
        if isinstance(environ, dict):
            # frozenset.isdisjoint probes the whole key set in C — measurably
            # cheaper than a Python-level membership loop, and this runs on
            # every request, sampled or not.
            return not _ENVIRON_PINPOINT_KEYS.isdisjoint(environ)
        for k in environ:
            if isinstance(k, str) and k.startswith("HTTP_PINPOINT_"):
                return True
    except Exception:  # noqa: BLE001
        return False
    return False


def _has_pinpoint_key(keys: Iterable[Any]) -> bool:
    """True if any key in ``keys`` (``str``/``bytes``) is a Pinpoint header."""
    try:
        for k in keys:
            # Prefilter on the first byte/char (0x50/0x70 = ``P``/``p``) before
            # paying for the slice + lower() copies.
            if isinstance(k, (bytes, bytearray)):
                if not k or k[0] not in (0x50, 0x70):
                    continue
                if k[:9].lower() == b"pinpoint-":
                    return True
            elif type(k) is str:
                if not k or k[0] not in "pP":
                    continue
                if k[:9].lower() == "pinpoint-":
                    return True
            elif str(k)[:9].lower() == "pinpoint-":
                return True
    except Exception:  # noqa: BLE001
        return False
    return False


# A header mapping (aiohttp ``CIMultiDict`` / tornado ``HTTPHeaders`` / plain
# dict) iterates its keys, so the key scan applies to it directly.
has_pinpoint_mapping = _has_pinpoint_key


def has_pinpoint_pairs(pairs: Iterable[Tuple[Any, Any]]) -> bool:
    """True if a sequence of ``(key, value)`` header pairs carries an upstream
    Pinpoint header.

    Used by messaging consumers (gRPC ``invocation_metadata``, Kafka record
    headers, …) whose keys may be ``str`` or ``bytes``. Checks keys only — no
    value decoding or reader construction — so the common no-context path stays
    cheap.
    """
    return _has_pinpoint_key(k for k, _ in pairs)


def parse_cookie_header(cookie_header: Any) -> dict:
    """Split a raw ``Cookie:`` header into a name/value mapping."""
    if not cookie_header:
        return {}
    out = {}
    for piece in str(cookie_header).split(";"):
        if "=" not in piece:
            continue
        key, _, value = piece.partition("=")
        key = key.strip()
        if key:
            out[key] = value.strip()
    return out


def get_remote_addr(remote_addr: str) -> str:
    """Strip the port from a ``host:port`` remote address.

    Python mirror of the fallback tail of
    ``pinpoint::HttpTracerUtil::getRemoteAddr``; the full XFF/X-Real-Ip
    resolution is :func:`_resolve_remote_addr`, which
    :func:`trace_http_server_request` runs first.
    """
    addr = (remote_addr or "").strip()
    if not addr:
        return ""
    # IPv6 with brackets: keep the literal ``[…]`` form.
    if addr.startswith("["):
        end = addr.find("]")
        if end != -1:
            return addr[: end + 1]
    last_colon = addr.rfind(":")
    if last_colon == -1:
        return addr
    first_colon = addr.find(":")
    if first_colon != last_colon:
        # Multiple colons → unbracketed IPv6, leave as is.
        return addr
    return addr[:last_colon]


# Fallback Config lists for direct helper calls/test doubles are resolved once
# per Agent. Production Span/SpanEvent paths use their native snapshot below.
# The cache is one immutable (agent, entries) tuple, swapped atomically and
# identity-checked on every read, so a racy re-init never serves a stale entry.
# Measured 2026-08-20: cache and a direct getattr chain land within tens of ns
# of each other per request (each way round on different machines), so this
# stays because removing it buys nothing, not because it measurably wins.
_hdr_cfg_cache: Tuple[Any, dict] = (None, {})

# Indices in the native SpanConfigSnapshot tuple the Agent fetches via
# get_config_snapshot() (refreshed on config-revision change) and stamps on
# each sampled span. Keeping the resolved config on the span also gives every
# SpanEvent the config generation its native parent was admitted under.
_SPAN_CONFIG_HEADER_INDEX = {
    "http_server_record_request_header": 5,
    "http_server_record_response_header": 6,
    "http_server_record_request_cookie": 7,
    "http_client_record_request_header": 8,
    "http_client_record_response_header": 9,
    "http_client_record_request_cookie": 10,
    "http_server_proxy_user_header_names": 13,
}

# Index 11 is the revision (read by Agent._load_span_config); the settings the
# Python layer enforces itself live past it — see
# agent._SNAPSHOT_SQL_TRACE_BIND_VALUE. Append-only, like the C++ tuple: an
# older binding or a test double returns a shorter tuple, and a missing index
# falls through to the Python Config below rather than reading as false.
_SPAN_CONFIG_BOOL_INDEX = {
    "sql_trace_bind_values": _SNAPSHOT_SQL_TRACE_BIND_VALUE,
    "http_client_record_url_query": 15,
    "http_server_record_request_param": 16,
}
_SNAPSHOT_REAL_IP_HEADER = 17
_SNAPSHOT_REAL_IP_EMPTY_VALUE = 18


def _snapshot_gate(config_attr: str, target=None) -> bool:
    """Resolve one boolean gate the Python layer enforces itself.

    Reads the native config snapshot the span was admitted under, so a config
    file and a hot reload reach the Python gate the same way an ``init()`` kwarg
    does. Falls back to the Python :class:`~pinpoint.config.Config` value for
    targets that carry no snapshot (test doubles, unsampled spans, direct helper
    calls), and to ``False`` if nothing resolves — these gate *recording* data,
    so the no-information answer must be "do not record".
    """
    try:
        index = _SPAN_CONFIG_BOOL_INDEX[config_attr]
        if target is None:
            target = current_span()
        if target is not None:
            span = getattr(target, "_span", target)
            snapshot = getattr(span, "_config_snapshot", ())
            if snapshot:
                try:
                    return bool(snapshot[index])
                except IndexError:
                    pass  # pre-index binding/double: use the Config fallback
        # No snapshot on this target: the Config fallback, cached per agent by
        # the header resolver (a non-list truthy value becomes a 1-tuple there).
        return _header_recording_configured(config_attr, target)
    except Exception:  # noqa: BLE001
        return False


def sql_trace_bind_values_enabled(target=None) -> bool:
    """Resolve the ``sql_trace_bind_values`` gate (see :func:`_snapshot_gate`)."""
    return _snapshot_gate("sql_trace_bind_values", target)


def _header_recording_config(config_attr: str, target=None) -> Tuple[tuple, bool]:
    """Resolve one recording config list to ``(names, dump_all)``.

    ``names`` is the configured header/cookie allow-list (empty = recording
    off, the default). ``dump_all`` is HEADERS-ALL mode: a single
    case-insensitive ``HEADERS-ALL`` entry records every header instead of an
    allow-list.
    """
    global _hdr_cfg_cache
    try:
        index = _SPAN_CONFIG_HEADER_INDEX.get(config_attr)
        if index is not None:
            if target is None:
                target = current_span()
            if target is not None:
                span = getattr(target, "_span", target)
                snapshot = getattr(span, "_config_snapshot", ())
                if snapshot:
                    names = snapshot[index]
                    return (
                        names,
                        len(names) == 1 and names[0].upper() == "HEADERS-ALL",
                    )

        # Compatibility fallback for direct helper calls and test doubles that
        # do not carry a native span snapshot.
        agent = get_agent()
        cached_agent, entries = _hdr_cfg_cache
        if agent is cached_agent:
            entry = entries.get(config_attr)
            if entry is not None:
                return entry
        else:
            entries = {}
        cfg = getattr(agent, "config", None)
        raw = getattr(cfg, config_attr, None)
        if isinstance(raw, (list, tuple)):
            names = tuple(str(n) for n in raw)
        elif raw:
            # Non-list truthy value: bool flags reuse this cache through
            # _header_recording_configured (see sql_bind_values_enabled).
            names = (str(raw),)
        else:
            names = ()
        entry = (names,
                 len(names) == 1 and names[0].upper() == "HEADERS-ALL")
        new_entries = dict(entries)
        new_entries[config_attr] = entry
        _hdr_cfg_cache = (agent, new_entries)
        return entry
    except Exception:  # noqa: BLE001
        return ((), False)


def _header_recording_configured(config_attr: str, target=None) -> bool:
    return bool(_header_recording_config(config_attr, target)[0])


def _proxy_header_recording_enabled(_target=None) -> bool:
    # ponytail: the native span snapshot does not expose this flag yet; switch
    # to it once the field exists, so file/profile reloads reach Python.
    try:
        return bool(getattr(getattr(get_agent(), "config", None),
                            "http_server_proxy_header_enable", True))
    except Exception:  # noqa: BLE001
        return True


def record_request_cookie_enabled(target=None) -> bool:
    """True when server request-cookie recording is configured (default off).

    Exposed so instrumentations can skip cookie-header parsing on the common
    recording-off path instead of parsing every sampled request's ``Cookie``
    header just for the recorder to discard it."""
    return _header_recording_configured(
        "http_server_record_request_cookie", target)


def record_response_header_enabled(target=None) -> bool:
    """True when server response-header recording is configured (default off)."""
    return _header_recording_configured(
        "http_server_record_response_header", target)


def _header_items(headers: HeadersLike) -> list:
    """All ``(name, value)`` pairs of a header container/reader, as str
    (HEADERS-ALL recording)."""
    for_each = getattr(headers, "for_each", None)
    if callable(for_each):
        items: list = []
        for_each(lambda k, v: items.append((k, v)))
        return items
    return _headers_items_lower(headers)[0]


def _record_header(
    target,
    anno_key: int,
    headers: HeadersLike,
    config_attr: str,
) -> None:
    """Resolve the configured allow-list, extract the header values, and buffer
    them as two-string annotations (under ``anno_key``) on the Span/SpanEvent
    wrapper. They flush with everything else in the one native finalize call at
    ``end()``, so native never iterates a Python reader."""
    # Empty gate first: the ``()`` default most callers pass must not pay the
    # config resolution below on every sampled request. Reader instances define
    # no ``__bool__``/``__len__`` so they stay truthy; an empty raw container
    # has nothing to record anyway.
    if not headers:
        return
    names, dump_all = _header_recording_config(config_attr, target)
    if not names:
        return

    try:
        annotate = target.annotate_string_string
        if dump_all:
            for k, v in _header_items(headers):
                annotate(anno_key, k, v)
            return
        if isinstance(headers, HeaderReader):
            get = headers.get  # case-insensitive by the reader contract
        else:
            lower = _headers_items_lower(headers)[1]
            get = lambda name: lower.get(name.lower())  # noqa: E731
        # The annotation carries the configured name, not the wire-case key.
        for name in names:
            value = get(name)
            if value is not None:
                annotate(anno_key, name, value)
    except Exception:  # noqa: BLE001
        pass


# ---- helpers ---------------------------------------------------------------


def _first_ip(value: str) -> str:
    """First entry of a comma-separated IP list, whitespace-trimmed."""
    comma = value.find(",")
    return (value[:comma] if comma != -1 else value).strip(" \t")


# Headers trusted for the client IP when nothing is configured: a deployment
# behind a proxy gets the real address out of the box. An explicit empty list
# turns header trust off and falls back to the socket address.
_DEFAULT_REAL_IP_HEADERS = ("X-Forwarded-For", "X-Real-Ip")
# RFC 7239 ``Forwarded: for=1.2.3.4;proto=https, for="[::1]:80"``.
_FORWARDED_FOR = re.compile(r'(?i:for)="?([^;,"]+)"?')


def _real_ip_config(target=None) -> Tuple[tuple, str]:
    """Resolve ``(header names, placeholder)`` for one span.

    Snapshot first (``Http.Server.RealIpHeader`` / ``RealIpEmptyValue``, the
    same generation the span was admitted under), so a config file and a hot
    reload reach this resolver; the Python Config for targets without one. An
    empty list from the snapshot is an explicit "trust no header", while a
    Config that carries no value at all keeps the XFF default above.
    """
    if target is not None:
        span = getattr(target, "_span", target)
        snapshot = getattr(span, "_config_snapshot", ())
        if snapshot:
            try:
                names = snapshot[_SNAPSHOT_REAL_IP_HEADER]
                empty = snapshot[_SNAPSHOT_REAL_IP_EMPTY_VALUE]
            except IndexError:
                pass  # pre-index binding/double: use the Config fallback
            else:
                return tuple(names), str(empty).strip().lower()
    cfg = getattr(get_agent(), "config", None)
    headers = getattr(cfg, "http_server_real_ip_header", None)
    if headers is None:
        headers = _DEFAULT_REAL_IP_HEADERS
    empty = getattr(cfg, "http_server_real_ip_empty_value", "") or ""
    return tuple(headers), str(empty).strip().lower()


def _resolve_remote_addr(reader, remote_addr: str, target=None) -> str:
    """Try the configured headers in order, skipping empty values and the
    configured placeholder (e.g. ``unknown``); take the first hop;
    ``Forwarded`` is parsed for its ``for=`` pair. No header resolves → the
    socket address, port-stripped."""
    try:
        names, empty_value = _real_ip_config(target)
    except Exception:  # noqa: BLE001
        names, empty_value = _DEFAULT_REAL_IP_HEADERS, ""
    for name in names:
        raw = reader.get(name)
        if not raw:
            continue
        value = str(raw)
        if name.lower() == "forwarded":
            m = _FORWARDED_FOR.search(value)
            ip = get_remote_addr(m.group(1)) if m else ""
        else:
            ip = _first_ip(value)
        if not ip or (empty_value and ip.lower() == empty_value):
            continue
        return ip
    return get_remote_addr(remote_addr or "")


_INT64_BOUND = 2 ** 63
_PROXY_UNSET = -1


def _parse_proxy_header(value: str) -> dict:
    """Space-delimited ``key=value`` tokens; any without ``=`` is skipped."""
    fields: dict = {}
    for token in value.split(" "):
        eq = token.find("=")
        if eq != -1:
            fields[token[:eq]] = token[eq + 1:]
    return fields


def _record_proxy_header(span, reader) -> None:
    """Record the builtin and configured proxy hops."""
    if not _proxy_header_recording_enabled(span):
        return
    if (value := reader.get("Pinpoint-ProxyApache")) is not None:
        fields = _parse_proxy_header(str(value))
        # Apache reports microseconds; drop three digits before parsing.
        received_time = _proxy_digits(fields.get("t", "")[:-3])
        duration = _proxy_duration_micros(fields.get("D", ""))
        idle = _proxy_percent(fields.get("i", ""))
        busy = _proxy_percent(fields.get("b", ""))
        if received_time > 0:
            span.annotate_long_iibbs(ANNOTATION_HTTP_PROXY_HEADER,
                                     received_time, 3, duration, idle, busy, "")
    if (value := reader.get("Pinpoint-ProxyNginx")) is not None:
        fields = _parse_proxy_header(str(value))
        received_time = _proxy_millis(fields.get("t", ""))
        duration = _proxy_duration_micros(fields.get("D", ""), nginx=True)
        if received_time > 0:
            span.annotate_long_iibbs(ANNOTATION_HTTP_PROXY_HEADER,
                                     received_time, 2, duration,
                                     _PROXY_UNSET, _PROXY_UNSET, "")
    if (value := reader.get("Pinpoint-ProxyApp")) is not None:
        fields = _parse_proxy_header(str(value))
        received_time = _proxy_digits(fields.get("t", ""))
        app = fields.get("app", "").strip(" \t\r\n")
        valid_app = (len(app) <= 30 and all(
            c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
            for c in app))
        if received_time > 0 and valid_app:
            span.annotate_long_iibbs(ANNOTATION_HTTP_PROXY_HEADER,
                                     received_time, 1, _PROXY_UNSET,
                                     _PROXY_UNSET, _PROXY_UNSET, app)

    names, _ = _header_recording_config("http_server_proxy_user_header_names", span)
    for name in names:
        # HEADERS-ALL has no special meaning here: only named lazy lookups.
        if not name or not (value := reader.get(name)):
            continue
        fields = _parse_proxy_header(str(value))
        received_time = _proxy_user_millis(fields.get("t", ""))
        if received_time <= 0:
            continue
        raw_duration = fields.get("D", "")
        duration = _proxy_duration_micros(raw_duration, nginx="." in raw_duration)
        # Native caps the configured name at 32 UTF-8 bytes, without cutting
        # a code point. The header's app/i/b fields do not apply to code 4.
        app = name.encode("utf-8")[:32].decode("utf-8", errors="ignore")
        span.annotate_long_iibbs(ANNOTATION_HTTP_PROXY_HEADER,
                                 received_time, 4, duration,
                                 _PROXY_UNSET, _PROXY_UNSET, app)


def _proxy_duration_micros(text: str, nginx: bool = False) -> int:
    value = _proxy_millis(text) * 1000 if nginx else _proxy_digits(text)
    return value if 0 < value < _INT32_BOUND else _PROXY_UNSET


def _proxy_percent(text: str) -> int:
    if not text or not text.isascii() or not text.isdecimal():
        return _PROXY_UNSET
    digits = text.lstrip("0")
    if len(digits) > 3:
        return _PROXY_UNSET
    value = int(digits or "0")
    return value if value <= 100 else _PROXY_UNSET


def _proxy_user_millis(text: str) -> int:
    """Infer the timestamp unit from the digit count: microseconds, then
    fractional seconds, then milliseconds."""
    raw = text.encode("utf-8")
    if len(raw) < 13:
        return 0
    if len(raw) >= 16:
        return _proxy_digits(raw[:-3].decode("ascii", errors="replace"))
    if "." in text:
        return _proxy_millis(text) if text.rfind(".") >= 10 else 0
    return _proxy_digits(text)


def _proxy_digits(text: str, bound: int = _INT64_BOUND) -> int:
    if not text or not text.isascii() or not text.isdecimal():
        return 0
    try:
        value = int(text)
    except ValueError:
        return 0
    return value if value < bound else 0


def _proxy_millis(text: str) -> int:
    # Exactly sec.mmm (the .mmm is required). Integer arithmetic, so there is
    # no float rounding and no accidental exponent/NaN syntax.
    seconds, dot, millis = text.rpartition(".")
    if not dot or len(millis) != 3:
        return 0
    return _proxy_digits((seconds or "0") + millis)


# Request-parameter limits: each key/value abbreviated to 64 chars, the joined
# string cut at 512 chars with a trailing "...".
_PARAM_EACH_LIMIT = 64
_PARAM_TOTAL_LIMIT = 512


def format_request_params(query_string: str) -> str:
    """``k=v&k=v`` from a raw query string, with the caps above applied."""
    parts = []
    total = 0
    for key, value in parse_qsl(query_string, keep_blank_values=True):
        if len(key) > _PARAM_EACH_LIMIT:
            key = key[:_PARAM_EACH_LIMIT] + "..."
        if len(value) > _PARAM_EACH_LIMIT:
            value = value[:_PARAM_EACH_LIMIT] + "..."
        item = f"{key}={value}"
        if total + len(item) + (1 if parts else 0) > _PARAM_TOTAL_LIMIT:
            parts.append("...")
            break
        parts.append(item)
        total += len(item) + (1 if len(parts) > 1 else 0)
    return "&".join(parts)


def trace_http_server_request(
    span,
    remote_addr: str,
    endpoint: str,
    request_headers: HeadersLike,
    cookie_reader: HeadersLike = None,
    query_string: str = "",
) -> None:
    """Open-side server tracing: remote address, endpoint, request headers.

    The ``X-Forwarded-For`` / ``X-Real-Ip`` resolution and ``Pinpoint-Proxy*``
    monitoring-header parsing run interpreter-side, and everything is buffered
    on the wrapper, flushing with the single native finalize call at
    ``span.end()`` — the request pays no native call here. Headers and cookies
    (only when the matching recording config is set, off by default) are
    extracted into buffered annotations that flush in that same call.
    """
    if span is None:
        return
    # HeadersReader(None) is an empty reader, so a caller with no headers still
    # gets remote/endpoint set and records nothing — no special branch needed.
    reader = (request_headers if isinstance(request_headers, HeaderReader)
              else HeadersReader(request_headers))

    try:
        span.set_remote_address(_resolve_remote_addr(reader, remote_addr, span))
        span.set_end_point(endpoint or "")
        if hasattr(span, "_acceptor_host") and not span._acceptor_host:
            span.set_acceptor_host(endpoint or "")
        _record_proxy_header(span, reader)
    except Exception:  # noqa: BLE001
        pass

    # _record_header extracts nothing past its config gate, so the
    # recording-off default records neither headers nor cookies.
    _record_header(span, ANNOTATION_HTTP_REQUEST_HEADER, reader,
                   "http_server_record_request_header")
    _record_header(span, ANNOTATION_HTTP_COOKIE, cookie_reader,
                   "http_server_record_request_cookie")
    # Opt-in: query strings routinely carry tokens and ids.
    if query_string and _snapshot_gate(
            "http_server_record_request_param", span):
        try:
            span.annotate_string(ANNOTATION_HTTP_PARAM,
                                 format_request_params(str(query_string)))
        except Exception:  # noqa: BLE001
            pass


def trace_http_server_response(
    span,
    url_pattern: str,
    method: str,
    status_code: Optional[int],
    response_headers: HeadersLike = None,
) -> None:
    """Close-side server tracing: status, url_stat, response headers.

    All of it is buffered on the wrapper and flushes at ``span.end()``;
    response headers only when their recording is configured.
    """
    if span is None:
        return
    # Gated so a caller with no status yet (middleware exit on early
    # disconnect) can't push status=0.
    if status_code:
        try:
            span.set_status_code(int(status_code))
            span.set_url_stat(
                str(url_pattern or ""), str(method or ""), int(status_code))
        except Exception:  # noqa: BLE001
            pass
    _record_header(
        span,
        ANNOTATION_HTTP_RESPONSE_HEADER,
        response_headers,
        "http_server_record_response_header",
    )


def trace_http_client_request(
    span_event,
    host: str,
    url: str,
    request_headers: HeadersLike,
    cookie_reader: HeadersLike = None,
) -> None:
    """Open-side client tracing: endpoint, destination, URL, request headers.

    Caches endpoint/destination/service-type metadata on the event, which
    ``SpanEvent.end()`` flushes with the rest of the record.
    """
    if span_event is None:
        return
    # Live-event probe: _ended is False only on an open tracer.SpanEvent —
    # missing on the null event (getattr default True), True after end().
    if getattr(span_event, "_ended", True):
        return
    try:
        span_event.set_service_type(SERVICE_TYPE_PYTHON_HTTP_CLIENT)
        span_event.set_end_point(host or "")
        span_event.set_destination(host or "")
        url = str(url or "")
        # The query string is where tokens and ids leak into the trace, so it
        # is dropped unless recording it was asked for.
        if not _snapshot_gate("http_client_record_url_query", span_event):
            url = url.partition("?")[0]
        span_event.annotate_string(ANNOTATION_HTTP_URL, url)
    except Exception:  # noqa: BLE001
        pass

    # _record_header extracts headers only past its config gate, so the
    # recording-off path pays nothing here.
    _record_header(
        span_event,
        ANNOTATION_HTTP_REQUEST_HEADER,
        request_headers,
        "http_client_record_request_header",
    )
    _record_header(
        span_event,
        ANNOTATION_HTTP_COOKIE,
        cookie_reader,
        "http_client_record_request_cookie",
    )


def trace_http_client_response(
    span_event,
    status_code: Optional[int],
    response_headers: HeadersLike = None,
) -> None:
    """Close-side client tracing: HTTP status annotation + response headers.

    Both are buffered on the wrapper, alongside the event identity metadata,
    and flush at ``SpanEvent.end()``.
    """
    if span_event is None:
        return
    # Live-event probe — see trace_http_client_request.
    if getattr(span_event, "_ended", True):
        return

    # No status yet (early disconnect) still flushes headers below.
    if status_code is not None:
        try:
            span_event.annotate_int(
                ANNOTATION_HTTP_STATUS_CODE, int(status_code))
        except Exception:  # noqa: BLE001
            pass
    _record_header(
        span_event,
        ANNOTATION_HTTP_RESPONSE_HEADER,
        response_headers,
        "http_client_record_response_header",
    )
