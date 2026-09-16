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

"""Helpers every instrumentation uses.

- ``wrap(module, target, wrapper)`` — install a wrapt function wrapper.
  Exceptions raised by the wrapper itself are swallowed (with a debug
  log) so an instrumentation bug can't crash the user's request; user
  code exceptions still propagate.

The ``safe_wrapper`` shim deliberately does **not** gate on
``agent.enabled``. Doing so at the wrapper layer is dangerous for one-
shot factory hooks whose result is cached by the framework (e.g.
``build_middleware_stack``): ``agent.enabled`` is False for the first
few seconds while the gRPC handshake runs, and a single bypass during
that window would memoize the unwrapped result and silently disable
tracing for the rest of the process. Individual instrumentations still
check ``agent.enabled`` themselves before opening a span — that's the
intended place for the gate, since they own the decision about whether
to do real work for a given request.
"""

from __future__ import annotations

import copy
import contextvars
import functools
import importlib
import inspect
import reprlib
import sys
import weakref
from collections.abc import Mapping
from typing import Any, Callable, Optional

import wrapt  # type: ignore[import-not-found]

from .. import http_helper as _http_helper
from .. import _log as _log_module
from .._log import get_logger
from ..agent import get_agent
from ..context import current_span, reset_current_span
from ..errors import safe_try
from ..propagator import inject_items
from ..service_type import (
    SERVICE_TYPE_PYTHON_HTTP_CLIENT,
    SERVICE_TYPE_PYTHON_HTTP_SERVER,
)
from ..tracer import Span

_log = get_logger("instrument")
_suppress_http_client_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
    "pinpoint_suppress_http_client_depth", default=0,
)


def no_current_span() -> bool:
    """Precheck for outbound hot paths (produce, publish, send): with no
    current span the wrapper would only re-issue the raw call."""
    return current_span() is None


def agent_disabled() -> bool:
    """Precheck for consume/poll hot paths, which open their own spans and
    need only a live agent."""
    agent = get_agent()
    return agent is None or not agent.enabled


def _with_precheck(inner: Callable[..., Any],
                   precheck: Optional[Callable[[], bool]]) -> Callable[..., Any]:
    """Route straight to the unwrapped target when ``precheck()`` says tracing
    is definitely off for this call — skipping the safe_wrapper machinery (its
    sentinel allocation and extra frames) on the no-tracing hot path. Same
    pattern the confluent_kafka subclass methods use. A raising precheck falls
    through to the guarded wrapper; it must never break the user's call."""
    if precheck is None:
        return inner

    @functools.wraps(inner)
    def _prechecked(wrapped, instance, args, kwargs):
        # Only precheck() may sit in the try: once the target is called, its
        # exception must propagate — swallowing it and falling through to
        # inner() would run the user's code a second time.
        try:
            fast_path = precheck()
        except Exception:  # noqa: BLE001
            fast_path = False
        if fast_path:
            return wrapped(*args, **kwargs)
        return inner(wrapped, instance, args, kwargs)

    return _prechecked


def safe_wrapper(fn: Callable[..., Any],
                 precheck: Optional[Callable[[], bool]] = None,
                 ) -> Callable[..., Any]:
    """Defensive wrap around an instrumentation callback.

    Calls ``fn(wrapped, instance, args, kwargs)`` unconditionally. If
    ``fn`` raises an ordinary exception *before it ever invoked the
    target*, we swallow it and fall back to calling the unwrapped target
    so the user request still completes. Non-``Exception`` failures (e.g.
    ``KeyboardInterrupt``, ``SystemExit``) always propagate.

    The fallback must never re-run user code: almost every synchronous
    wrapper calls the target itself and lets the target's exception
    propagate (``span_event_scope`` records then re-raises). A blind
    ``return wrapped(*args, **kwargs)`` in the ``except`` clause would then
    execute the user's function a *second* time and swallow the original
    exception — double INSERTs, views running twice, lost messages. To
    avoid that we hand ``fn`` a sentinel-wrapped target that records the
    moment control crosses into user code:

    - target never entered  → genuine instrumentation bug → fall back and
      call the real target once.
    - target entered and raised → that is the user's own exception →
      re-raise it unchanged (never re-run).
    - target returned, then instrumentation raised afterwards → swallow
      the instrumentation bug and return the value the target produced.

    Note this only guards *synchronous* wrappers. For ``async def``
    callbacks ``fn(...)`` merely builds a coroutine, so nothing here
    executes user code and the sentinel state is never consulted —
    those get the slimmer guard below, with no per-call state at all.

    At DEBUG the entry and exit of every instrumentation callback are logged,
    which is how you tell "the hook never ran" from "the hook ran and recorded
    nothing". Only the callback's name is logged — never the target's
    arguments, which are user data. The gate is the module-level flag
    :data:`pinpoint._log.debug_enabled`, resolved when the log level is
    configured, so a disabled trace costs one attribute load per call rather
    than a logger level lookup.
    """

    if inspect.iscoroutinefunction(fn):
        # Calling an async fn only builds the coroutine; user code runs later, at
        # await time. So an exception here is an instrumentation bug that predates the
        # target running, and the plain fallback is safe — no sentinel needed.
        @functools.wraps(fn)
        def _async_wrapper(wrapped, instance, args, kwargs):
            try:
                if _log_module.debug_enabled:
                    # No "after" here: this call only builds the coroutine, so
                    # returning from it says nothing about the hook finishing.
                    _log.debug("interceptor before %s (async)", fn.__qualname__)
                return fn(wrapped, instance, args, kwargs)
            except BaseException as exc:
                if not isinstance(exc, Exception):
                    raise
                _log.debug("instrumentation exception in %s",
                           fn.__qualname__, exc_info=True)
                return wrapped(*args, **kwargs)

        return _with_precheck(_async_wrapper, precheck)

    @functools.wraps(fn)
    def _wrapper(wrapped, instance, args, kwargs):
        # One ``_SafeSentinel`` per instrumented synchronous call: a single
        # ``__slots__`` allocation that is both the sentinel target and its state.
        sentinel = _SafeSentinel(wrapped)
        try:
            if _log_module.debug_enabled:
                _log.debug("interceptor before %s", fn.__qualname__)
                result = fn(sentinel, instance, args, kwargs)
                _log.debug("interceptor after %s", fn.__qualname__)
                return result
            return fn(sentinel, instance, args, kwargs)
        except BaseException as exc:
            if not isinstance(exc, Exception):
                raise
            if sentinel.returned:
                # User code already ran to completion; the failure is in
                # post-call instrumentation. Swallow it, hand back the
                # value the target produced.
                _log.debug("post-call instrumentation exception in %s",
                           fn.__qualname__, exc_info=True)
                return sentinel.result
            if sentinel.entered:
                # The exception came out of the user's own code — propagate
                # it verbatim; re-running the target would be catastrophic.
                raise
            _log.debug("instrumentation exception in %s",
                       fn.__qualname__, exc_info=True)
            return wrapped(*args, **kwargs)

    return _with_precheck(_wrapper, precheck)


class _SafeSentinel:
    """Sentinel-wrapped target handed to a synchronous ``safe_wrapper`` body.

    It records the moment control crosses into user code (``entered``) and the
    value the target produced (``result`` / ``returned``), so ``safe_wrapper``
    can tell an instrumentation bug from the user's own exception without ever
    re-running the target. A ``__slots__`` class keeps that to one allocation
    per call on this ubiquitous hot path.
    """

    __slots__ = ("_wrapped", "entered", "returned", "result")

    def __init__(self, wrapped) -> None:
        self._wrapped = wrapped
        self.entered = False
        self.returned = False
        self.result = None

    def __call__(self, *args, **kwargs):
        self.entered = True
        result = self._wrapped(*args, **kwargs)
        self.result = result
        self.returned = True
        return result

    @property
    def __wrapped__(self):
        # Makes the sentinel transparent to callers that introspect ``wrapped`` (pika
        # resolves its consumer-callback slot from the signature): ``inspect.signature``
        # /``unwrap`` follow this to the real target rather than seeing
        # ``(*args, **kwargs)``. A class-level descriptor, so no per-call cost.
        return self._wrapped


# Stamped on every wrapper pinpoint installs. wrapt keeps our wrapper on the
# installed ``FunctionWrapper`` as ``_self_wrapper``, so checking the stamp there
# recognises *our own* patch — making a re-install a no-op without also skipping
# when a third-party APM wrapped the same callable.
_PINPOINT_WRAPPER_ATTR = "__pinpoint_wrapper__"
_PINPOINT_WRAPPER_OWNER_ATTR = "__pinpoint_wrapper_owner__"
_wrapper_install_owner: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "pinpoint_wrapper_install_owner", default=None,
)


def begin_wrapper_install(owner: Any):
    """Associate subsequent :func:`wrap` calls with one instrumentor."""
    return _wrapper_install_owner.set(owner)


def end_wrapper_install(token) -> None:
    """End a wrapper-install registration scope."""
    _wrapper_install_owner.reset(token)


def record_wrapper_target(module: str, target: str) -> None:
    """Record a directly installed wrapper on the active instrumentor."""
    owner = _wrapper_install_owner.get()
    if owner is None:
        return
    try:
        owner._record_wrapper_target(module, target)
    except Exception:  # noqa: BLE001
        _log.debug("failed to record wrapper target %s.%s", module, target,
                   exc_info=True)


def mark_pinpoint_wrapper(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Tag ``fn`` so :func:`already_wrapped` recognises a pinpoint patch."""
    try:
        setattr(fn, _PINPOINT_WRAPPER_ATTR, True)
        setattr(fn, _PINPOINT_WRAPPER_OWNER_ATTR, _wrapper_install_owner.get())
    except Exception:  # noqa: BLE001
        pass
    return fn


def _is_pinpoint_wrapper(obj: Any) -> bool:
    """True when ``obj`` is a wrapt ``FunctionWrapper`` that pinpoint installed.

    wrapt stashes the wrapper callable we handed it on the ``FunctionWrapper`` as
    ``_self_wrapper``; :func:`mark_pinpoint_wrapper` stamps that callable with
    :data:`_PINPOINT_WRAPPER_ATTR`. Checking the stamp there recognises *our own*
    patch on a target without being fooled by a third-party APM's wrapper.
    """
    wrapper = getattr(obj, "_self_wrapper", None)
    return getattr(wrapper, _PINPOINT_WRAPPER_ATTR, False) is True


def _resolve_target(module: str, target: str) -> Any:
    """Object currently installed at ``module.target``, or ``None``.

    Any resolution failure (module not importable, attribute missing) reads as
    "not present": callers treat ``None`` as "nothing of ours is installed".
    """
    try:
        obj: Any = sys.modules.get(module) or importlib.import_module(module)
        for part in target.split("."):
            obj = getattr(obj, part)
        return obj
    except Exception:  # noqa: BLE001
        return None


def already_wrapped(module: str, target: str) -> bool:
    """Return True when ``module.target`` is already patched by pinpoint.

    Resolves the (possibly dotted) attribute and inspects wrapt's
    ``_self_wrapper`` for the :data:`_PINPOINT_WRAPPER_ATTR` stamp.
    """
    return _is_pinpoint_wrapper(_resolve_target(module, target))


def wrap(module: str, target: str, wrapper: Callable[..., Any],
         precheck: Optional[Callable[[], bool]] = None) -> bool:
    """Install a wrapt wrapper on ``module.target`` guarded by ``safe_wrapper``.

    ``precheck`` (see :func:`no_current_span` / :func:`agent_disabled`) makes
    per-call hot paths skip the wrapper machinery entirely when tracing is
    definitely off for the call.

    Idempotent: if pinpoint already wrapped ``module.target`` (a repeated
    ``instrument()`` or an instrument→uninstrument→instrument cycle), the
    re-wrap is skipped so wrappers never stack.

    While a :class:`BaseInstrumentor` install scope is active, the target is
    also registered for symmetric uninstrument. Returns whether the target is
    installed, counting one the same (or an ownerless) Pinpoint install
    already put there.
    """
    try:
        installed = _resolve_target(module, target)
        if _is_pinpoint_wrapper(installed):
            _log.debug("skip re-wrap of %s.%s — already instrumented",
                       module, target)
            # A retry by the same owner still needs the target in its uninstall
            # registry; an ownerless wrapper is adopted by the active instrumentor.
            installed_owner = getattr(
                getattr(installed, "_self_wrapper", None),
                _PINPOINT_WRAPPER_OWNER_ATTR,
                None,
            )
            active_owner = _wrapper_install_owner.get()
            if installed_owner is None or installed_owner is active_owner:
                record_wrapper_target(module, target)
            return True
        wrapt.wrap_function_wrapper(
            module, target,
            mark_pinpoint_wrapper(safe_wrapper(wrapper, precheck)),
        )
        record_wrapper_target(module, target)
        return True
    except Exception:  # noqa: BLE001
        _log.debug("failed to wrap %s.%s", module, target, exc_info=True)
        return False


# Defensive bound on how far we walk a ``__wrapped__`` chain — real stacks are a
# handful of layers deep; this only guards against a pathological proxy whose
# ``__wrapped__`` never terminates.
_MAX_WRAP_DEPTH = 64


def unwrap_pinpoint(current: Any) -> Any:
    """Value an instrumented slot should hold once *only* pinpoint's wrapt layer
    is removed from its ``__wrapped__`` chain.

    ``_uninstrument`` must peel off pinpoint's own wrapper without disturbing a
    wrapper another library — another APM, a tracing SDK — may have layered on
    the same target: the mirror of the ownership check :func:`wrap` performs on
    install. Pinpoint's layer is identified by :func:`_is_pinpoint_wrapper`.
    Three cases:

    * Outermost layer is pinpoint's → return its ``__wrapped__`` so the caller
      reassigns the slot one layer down (the classic restore, now guarded). Any
      foreign wrapper we were layered *over* is preserved as that inner value.
    * Pinpoint's layer sits *inside* the chain (a foreign wrapper is outermost)
      → splice ours out in place by re-pointing the enclosing layer's
      ``__wrapped__`` past it, then return ``current`` unchanged so the foreign
      outermost wrapper stays installed. If the enclosing layer refuses the
      re-point, leave the chain intact — never strip a foreign wrapper — and log
      at debug.
    * Pinpoint's layer isn't in the chain at all → return ``current`` unchanged.

    Callers reassign the slot only when the result is a *different* object than
    ``current``; the splice path mutates in place and reports "no reassignment".
    """
    if _is_pinpoint_wrapper(current):
        return getattr(current, "__wrapped__", current)

    prev = current
    for _ in range(_MAX_WRAP_DEPTH):
        node = getattr(prev, "__wrapped__", None)
        if node is None:
            return current
        if _is_pinpoint_wrapper(node):
            inner = getattr(node, "__wrapped__", None)
            if inner is not None:
                try:
                    prev.__wrapped__ = inner
                except Exception:  # noqa: BLE001
                    _log.debug("cannot splice out pinpoint wrapper; leaving the "
                               "wrapper chain untouched", exc_info=True)
            return current
        prev = node
    return current


def restore_pinpoint_target(module: str, target: str) -> None:
    """Undo a :func:`wrap` on a dotted target, removing *only* pinpoint's
    wrapt layer via :func:`unwrap_pinpoint`.

    Resolves the installed slot the same way :func:`already_wrapped` /
    :func:`wrap` do — a module-level attribute for a bare name, otherwise a
    class attribute read from the class ``__dict__`` (never an inherited one). A
    missing module or attribute is a no-op, and a wrapper another library layered
    on the same target is left installed and functional.
    """
    mod = sys.modules.get(module)
    if mod is None:
        return
    parts = target.split(".")
    if not parts:
        return
    target_holder: Any = mod
    try:
        for part in parts[:-1]:
            target_holder = getattr(target_holder, part)
    except Exception:  # noqa: BLE001
        return
    key = parts[-1]
    if isinstance(target_holder, type):
        current = target_holder.__dict__.get(key)
    else:
        current = getattr(target_holder, key, None)
    if current is None:
        return
    restored = unwrap_pinpoint(current)
    if restored is current:
        return
    try:
        setattr(target_holder, key, restored)
    except Exception:  # noqa: BLE001
        _log.debug("failed to restore %s.%s", module, target, exc_info=True)


class suppress_http_client_instrumentation:
    """Temporarily suppress lower-level HTTP client wrappers in this context.

    A plain ``__enter__``/``__exit__`` class rather than a ``@contextmanager``:
    this scope rides on every ``requests``/``elasticsearch`` call, sampled or
    not, and the generator + _GeneratorContextManager pair the decorator
    allocates per call is pure overhead. Same treatment as ``span_event_scope``.
    """

    __slots__ = ("_token",)

    def __enter__(self) -> None:
        depth = _suppress_http_client_depth.get()
        self._token = _suppress_http_client_depth.set(depth + 1)
        return None

    def __exit__(self, exc_type, exc_val, tb) -> bool:
        _suppress_http_client_depth.reset(self._token)
        return False


def http_client_instrumentation_suppressed() -> bool:
    return _suppress_http_client_depth.get() > 0


# DB instrumentations gate bind-VALUE capture on this (default off: values
# routinely hold PII/secrets). Resolved from the target span's native config
# snapshot; pass the span/event already in hand to skip current_span().
sql_bind_values_enabled = _http_helper.sql_trace_bind_values_enabled


def inject_http_headers(span: Optional[Span], headers: Any) -> None:
    """Inject trace headers into a private per-send header mapping.

    Callers must pass headers from :func:`copy_http_request`; mutating a shared
    request and restoring it after the send is racy when that request is sent
    concurrently. Never raises: a failed pair only means it is not propagated.
    """
    if headers is None or span is None:
        return
    try:
        items = inject_items(span)  # already a tuple; no copy needed
    except Exception:  # noqa: BLE001
        return
    for key, value in items:
        try:
            headers[str(key)] = str(value)
        except Exception:  # noqa: BLE001
            continue


def copy_http_request(request: Any) -> Any:
    """Return a shallow request copy with an independent header mapping.

    A request body or stream intentionally remains shared: duplicating one-shot
    payloads would change client semantics. ``None`` means a safe copy could
    not be made, in which case instrumentation should send the original request
    without injecting headers.
    """
    if request is None:
        return None

    try:
        # HTTPX Request defines pickle hooks that deliberately detach streams.
        # ``copy.copy(request)`` invokes those hooks, so bypass them for normal
        # Python objects and clone their instance state directly instead.
        state = getattr(request, "__dict__", None)
        if isinstance(state, dict):
            request_copy = object.__new__(type(request))
            request_copy.__dict__.update(state)
        else:
            copier = getattr(request, "copy", None)
            request_copy = copier() if callable(copier) else copy.copy(request)
        if request_copy is None or request_copy is request:
            return None

        headers = getattr(request, "headers", None)
        if headers is None:
            return request_copy

        copier = getattr(headers, "copy", None)
        headers_copy = copier() if callable(copier) else copy.copy(headers)
        if headers_copy is headers:
            headers_copy = type(headers)(headers)
        if headers_copy is headers:
            return None

        setattr(request_copy, "headers", headers_copy)
        return request_copy
    except Exception:  # noqa: BLE001
        return None


def request_with_trace_headers(span: Optional[Span], request: Any) -> Any:
    """Prepare an isolated request for one traced send, without raising."""
    request_copy = copy_http_request(request)
    if request_copy is None:
        return request
    inject_http_headers(span, getattr(request_copy, "headers", None))
    return request_copy


def open_client_send(span, operation, request, args, kwargs, annotate):
    """Open the span event for one outbound HTTP send and prepare its request.

    Shared by the ``send``-shaped clients (requests, httpx sync/async). The
    event is opened first — for an unsampled span it is the shared no-op — so
    the trace context injected next reflects this RPC's position in the
    trace. The headers go into a private per-send copy: mutating the user's
    request and restoring it races when one request is sent concurrently.
    ``annotate(event, send_request)`` runs only when sampled. Returns
    ``(event, send_request, send_args, send_kwargs, sampled)``.

    On a setup failure the event is ended before the exception propagates, so
    the caller's untraced fallback leaks nothing: safe_wrapper cannot end an
    event it never saw, and the async wrappers have no safe_wrapper at all.
    """
    event = span.new_span_event(
        operation, service_type=SERVICE_TYPE_PYTHON_HTTP_CLIENT)
    try:
        send_request = request_with_trace_headers(span, request)
        send_args, send_kwargs = replace_arg(
            args, kwargs, 0, "request", send_request)
        sampled = span_is_sampled(span)
        if sampled:
            annotate(event, send_request)
    except Exception:
        end_quietly(event)
        raise
    return event, send_request, send_args, send_kwargs, sampled


def replace_arg(args, kwargs, index, name, value):
    """Return ``(args, kwargs)`` with ``value`` written where the callee will
    read it: positionally at ``index`` when the call supplied it that way,
    else under keyword ``name``. Only the container being modified is copied,
    so a ``safe_wrapper`` retry never sees the injected value."""
    if len(args) > index:
        new_args = list(args)
        new_args[index] = value
        return tuple(new_args), kwargs
    new_kwargs = dict(kwargs)
    new_kwargs[name] = value
    return args, new_kwargs


@safe_try
def annotate_client_response(event, response) -> None:
    """Record an outbound response's status and headers on ``event``.

    ``status_code`` is the requests/httpx attribute; ``status`` is the
    aiohttp/urllib3 one. Swallowing is deliberate: this runs after the send
    completed, so a bad response object must not fail the user's call.
    """
    status = headers = None
    if response is not None:
        status = getattr(response, "status_code", None)
        if status is None:
            status = getattr(response, "status", None)
        headers = getattr(response, "headers", None)
    _http_helper.trace_http_client_response(event, status, headers)


def rebind_http_response_request(response, original, sent) -> None:
    """Preserve the public response.request identity after an isolated send."""
    if response is None or sent is original:
        return
    try:
        if getattr(response, "request", None) is sent:
            response.request = original
    except Exception:  # noqa: BLE001
        pass


def span_is_sampled(span: Optional[Span]) -> bool:
    """Return False only when the span explicitly says it is unsampled."""
    if span is None:
        return False
    try:
        return bool(span.sampled)
    except Exception:  # noqa: BLE001
        return True


def annotate_server_request(span, remote, host, headers,
                            cookie_keys=("Cookie", "cookie"),
                            query_string: str = "") -> None:
    """Server-side request stamp shared by the HTTP transports (wsgi/asgi/
    aiohttp/tornado): service type plus the shared request trace
    (X-Forwarded-For resolution, the RecordRequestHeader allow-list).

    ``cookie_keys`` are tried in order against ``headers`` — only when cookie
    recording is enabled, keeping the header lookup off the disabled path."""
    try:
        span.set_service_type(SERVICE_TYPE_PYTHON_HTTP_SERVER)
    except Exception:  # noqa: BLE001
        pass
    cookie_reader = None
    if _http_helper.record_request_cookie_enabled(span):
        raw = None
        for key in cookie_keys:
            raw = headers.get(key)
            if raw:
                break
        cookie_reader = _http_helper.parse_cookie_header(raw)
    _http_helper.trace_http_server_request(
        span, remote, host, headers, cookie_reader=cookie_reader,
        query_string=query_string)


def end_server_span(span, url_pattern, method, status_code,
                    response_headers=(), sampled=True) -> None:
    """Shared tail of every HTTP-server transport: full response trace on the
    sampled path; URL-stat aggregation only on the unsampled path, skipping
    the status=0 sentinel (app raised before a response started — that bucket
    is spurious); then ``span.end()``."""
    try:
        if sampled:
            _http_helper.trace_http_server_response(
                span, url_pattern, method, status_code, response_headers or ())
        elif status_code:
            span.set_url_stat(url_pattern, method, int(status_code))
    except Exception:  # noqa: BLE001
        # Annotation failure = a thinner trace; a skipped end() = a root span
        # that never finishes. The callers are @safe_try, so without this the
        # exception would silently take end() with it.
        _log.debug("end_server_span annotation failed", exc_info=True)
    finally:
        span.end()


def truncate_text(text: str, max_len: int) -> str:
    """``text`` clipped to at most ``max_len`` characters, ellipsized when it
    had to be cut.

    Below a cap of four the marker does not fit inside the cap, so hard-clip
    instead: the cap is the point, and returning "..." for max_len=2 overruns
    the annotation budget the caller sized."""
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if max_len < 4:
        return text[:max_len]
    return text[: max_len - 3] + "..."


class _BoundedRepr(reprlib.Repr):
    """``reprlib.Repr`` that slices bytes *before* repring them.

    Stock ``reprlib`` has no ``repr_bytes``/``repr_bytearray`` handler, so
    bytes fall through to ``repr_instance`` — which materializes the repr of
    the **entire** value (O(len) CPU plus a transient string ~3-4x its size
    for a BLOB query param) and only then truncates. ``repr_str`` already
    slices first; give bytes the same treatment.
    """

    def repr_bytes(self, x, level):
        if len(x) > self.maxstring:
            return repr(x[: self.maxstring]) + "..."
        return repr(x)

    repr_bytearray = repr_bytes


@functools.lru_cache(maxsize=32)
def _repr_for(max_len: int) -> _BoundedRepr:
    # One configured instance per max_len (call sites use a couple of fixed
    # caps). Repr instances hold no per-call state, so sharing is safe.
    r = _BoundedRepr()
    r.maxstring = r.maxother = max_len
    r.maxlist = r.maxtuple = r.maxdict = r.maxset = r.maxfrozenset = 8
    return r


def limited_repr(value: Any, max_len: int = 1024) -> str:
    """Bound repr output before it can allocate huge annotation strings."""
    return truncate_text(_repr_for(max_len).repr(value), max_len)


def limited_structure(value: Any, *, max_chars: int, max_depth: int,
                      max_items: int, max_string: int):
    """Bounded deep copy of a (mostly) jsonable structure, for previews.

    Applies a depth cap, a per-container item cap, a per-string truncation
    (``max_string``), and — because the item and depth caps alone are
    multiplicative — a global character budget (``max_chars``) plus a node
    budget that also bounds empty strings and structural-only values.
    Once a budget is spent, remaining subtrees collapse to ``"..."`` /
    ``"+N more"`` markers, so the copy and the eventual ``json.dumps``
    input are bounded by the output cap rather than the input size.
    Non-jsonable leaves degrade to :func:`limited_repr`.
    """
    budget = [max_chars, max_chars]  # [remaining chars, remaining nodes]

    def walk(value, depth):
        budget[1] -= 1
        if budget[0] <= 0 or budget[1] < 0 or depth >= max_depth:
            return "..."
        if value is None or isinstance(value, (bool, int, float)):
            budget[0] -= 8
            return value
        if isinstance(value, str):
            out = truncate_text(value, max_string)
            budget[0] -= max(len(out), 1)
            return out
        if isinstance(value, (bytes, bytearray)):
            out = bytes(value[: max_string * 4]).decode("utf-8", "replace")
            budget[0] -= max(len(out), 1)
            return out
        # Mapping, not dict: a driver's own document type (pymongo's
        # RawBSONDocument, SON) is a Mapping but not a dict subclass, and
        # falling through to limited_repr would materialize the repr of the
        # *whole* value — RawBSONDocument's embeds its entire raw payload, so
        # a 16 MB document builds a ~100 MB string only to slice 4 KiB off it.
        # Walking it structurally stays inside the item and char budgets.
        if isinstance(value, Mapping):
            limited = {}
            for idx, (key, item) in enumerate(value.items()):
                if idx >= max_items or budget[0] <= 0:
                    limited["..."] = f"+{len(value) - idx} more"
                    break
                key_text = truncate_text(str(key), max_string)
                budget[0] -= max(len(key_text), 1)
                limited[key_text] = walk(item, depth + 1)
            return limited
        if isinstance(value, (list, tuple)):
            limited = []
            for idx, item in enumerate(value):
                if idx >= max_items or budget[0] <= 0:
                    limited.append(f"...+{len(value) - idx} more")
                    break
                limited.append(walk(item, depth + 1))
            return limited
        out = limited_repr(value, max_string)
        budget[0] -= len(out)
        return out

    return walk(value, 0)


def end_quietly(event) -> None:
    """End a span event opened on a setup path that then failed, swallowing
    anything ``end()`` raises.

    An instrumentation that creates an event and then bails to the untraced
    call must not leave it open: it stays the innermost entry on the span's
    event stack, so later outbound injection rides the wrong event and later
    events nest under it until the root span's ``end()`` drains the stack.
    """
    if event is None:
        return
    try:
        event.end()
    except Exception:  # noqa: BLE001
        _log.debug("end_quietly failed", exc_info=True)


def reset_quietly(token) -> None:
    """Detach a span from the contextvar, swallowing the ``ValueError`` a
    token created in another Context raises: a consumer driven from a
    different task/thread than the one closing it, or a streaming response
    generator finalized (``close()``/GC) elsewhere. The ``span.end()`` that
    follows must still run, or the native root span leaks."""
    if token is None:
        return
    try:
        reset_current_span(token)
    except Exception:  # noqa: BLE001
        pass


def close_span_scope(scope) -> None:
    """End the span/event of an open ``(span, span_event, token)`` consumer
    scope and detach it from the contextvar. Always returns ``None``; never
    raises."""
    if scope is None:
        return None
    span, span_event, token = scope
    reset_quietly(token)
    end_quietly(span_event)
    end_quietly(span)
    return None


def mint_async_child(span, operation):
    """Linked async child for handing the current work to another thread —
    the ownership rules of ``pinpoint.async_trace``: minted on the thread
    that owns ``span`` (inside a hand-off event so the collector links it)
    and ended exactly once by whichever side wins the latch (see
    :func:`claim_handoff`). ``None`` when the mint fails; the caller runs
    untraced."""
    try:
        with span.new_span_event(operation):
            return span.new_async_span(operation)
    except Exception:  # noqa: BLE001
        _log.debug("%s span hand-off failed", operation, exc_info=True)
        return None


def claim_handoff(latch):
    """Take the child out of a one-element hand-off ``latch`` (a list), or
    ``None`` when the other side already did. ``list.pop`` is one GIL-atomic
    op, so exactly one of {worker, dispatcher cleanup} wins and the child is
    never driven by two threads."""
    try:
        return latch.pop()
    except IndexError:
        return None


def reclaim_handoff(latch) -> None:
    """Dispatcher-side cleanup: end the child if no worker claimed it —
    dispatch failed or was cancelled before the worker ran. A claimed child
    belongs to the worker's ``with`` block."""
    end_quietly(claim_handoff(latch))


def record_exception_on_span(span: Optional[Span], exc: BaseException) -> None:
    if span is None or not isinstance(exc, Exception):
        return
    try:
        span.set_error(exc)
    except Exception:  # noqa: BLE001
        _log.debug("record_exception failed", exc_info=True)


# Same contract for span events; both targets expose the same set_error API.
record_exception_on_event = record_exception_on_span


# Weak-key fallbacks for objects that reject attribute assignment (C types
# without an instance dict — psycopg2's connection — and __slots__ classes like
# asyncpg's Connection): without them those would recompute on every command,
# forever. One per stamped attribute name; entries die with the object.
_weak_memo: dict = {}


def memoize_on(obj, attr: str, resolve, *args):
    """``resolve(obj, *args)`` computed once per object and stamped on
    ``obj.<attr>``.

    For a value fixed for the object's life (a connection's address, a pool's
    endpoint) that a hot path needs per call: resolved on the first call, then
    re-read from the attribute. Objects that reject the attribute fall back to
    a weak-key cache; only one that is also not weakref-able recomputes per
    call. A falsy result is returned but not cached."""
    try:
        cached = getattr(obj, attr, None)
    except Exception:  # noqa: BLE001
        cached = None
    if cached is None:
        fallback = _weak_memo.get(attr)
        if fallback is not None:
            try:
                cached = fallback.get(obj)
            except Exception:  # noqa: BLE001
                cached = None
    if cached is not None:
        return cached
    value = resolve(obj, *args)
    if value:
        try:
            setattr(obj, attr, value)
        except Exception:  # noqa: BLE001
            try:
                _weak_memo.setdefault(attr, weakref.WeakKeyDictionary())[obj] = value
            except Exception:  # noqa: BLE001
                pass
    return value


def _format_endpoint(connection, resolve) -> Optional[str]:
    try:
        host, port = resolve(connection)
    except Exception:  # noqa: BLE001
        return None
    if not host:
        return None
    return f"{host}:{port}" if port else str(host)


def cached_endpoint(connection, resolve) -> Optional[str]:
    """``host:port`` of ``connection``, memoized on it (see :func:`memoize_on`).
    ``resolve(connection)`` returns ``(host, port)``; a failing resolver
    yields ``None``."""
    if connection is None:
        return None
    return memoize_on(connection, "_pinpoint_endpoint", _format_endpoint, resolve)


def mro_has(obj, names, module_prefix=None) -> bool:
    """True when ``type(obj).__mro__`` contains a class whose ``__name__`` is
    in ``names`` (and, when given, whose ``__module__`` starts with
    ``module_prefix``). Matching by name/module instead of importing the
    framework keeps callers import-free and still catches user subclasses."""
    if isinstance(names, str):
        names = (names,)
    try:
        for klass in type(obj).__mro__:
            if klass.__name__ in names and (
                not module_prefix
                or str(getattr(klass, "__module__", "")).startswith(module_prefix)
            ):
                return True
    except Exception:  # noqa: BLE001
        pass
    return False


def http_exception_status(exc, names, *, module_prefix=None,
                          status_attrs=("status_code",)):
    """Integer HTTP status carried by ``exc`` when it is a framework
    HTTP-response exception, else ``None``.

    Web frameworks let a handler ``raise`` a special exception to *produce* a
    non-2xx response (``flask.abort(404)`` → werkzeug ``NotFound``,
    ``falcon.HTTPError``, ``pyramid.httpexceptions.HTTPNotFound``, …). A sub-500
    one is normal control flow, not a failure; callers use this to decide
    whether to record it as a span-event error.

    Walks ``type(exc).__mro__`` for a class whose ``__name__`` is in ``names``
    (and, when ``module_prefix`` is given, whose ``__module__`` starts with it).
    Matching by name/module rather than importing the framework keeps this
    import-free and still catches user subclasses; the module gate disambiguates
    the many unrelated classes called ``HTTPException``/``HTTPError`` across
    libraries (``urllib.error``, ``requests``, starlette, …) that would
    otherwise be misclassified. On a match, returns the first present of
    ``status_attrs`` as an int — accepting a plain int / ``IntEnum``
    (werkzeug/pyramid ``.code``) *and* falcon's ``"404 Not Found"`` status
    string. A matched exception with no readable status yields ``0`` (its
    callers treat that as sub-500 control flow, mirroring the aiohttp
    classifier). Returns ``None`` when ``exc`` is not one of the named classes,
    so callers can tell "not an HTTP-response exception" from "a 5xx one".
    """
    if not mro_has(exc, names, module_prefix=module_prefix):
        return None
    for attr in status_attrs:
        raw = getattr(exc, attr, None)
        if raw is None:
            continue
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
        try:
            # falcon .status is "404 Not Found" — take the leading int.
            return int(str(raw).split(" ", 1)[0])
        except (TypeError, ValueError):
            continue
    return 0


def callable_operation_name(fn, default=""):
    """User-meaningful span-event name for a handler callable.

    Strips ``functools.partial`` layers and wrapt/``functools.wraps`` wrappers
    so the user's source-level name surfaces rather than
    ``decorator.<locals>._wrapper``, then takes ``__qualname__`` (or
    ``__name__``). A callable *object* has no name of its own, so it is named
    after its class. Returns ``default`` for ``None`` or when nothing
    resolves."""
    if fn is None:
        return default
    while isinstance(fn, functools.partial):
        fn = fn.func
    unwrapped = getattr(fn, "__wrapped__", fn)
    name = (getattr(unwrapped, "__qualname__", None)
            or getattr(unwrapped, "__name__", None))
    if name:
        return name
    cls = type(unwrapped)
    return (getattr(cls, "__qualname__", None)
            or getattr(cls, "__name__", None)
            or default)


# callable_operation_name memoized: view/route handlers are fixed at
# app-definition time, so the partial/__wrapped__ walk is paid once per
# handler, not per request.
#
# Caller contract: pass only callables that live as long as the app — view
# functions, route endpoints, dependant.call. The cache holds its keys
# strongly, so a per-request bound method or a freshly built partial would pin
# up to 2048 of them (and whatever their closures capture) until evicted.
@functools.lru_cache(maxsize=2048)
def _cached_operation_name(fn) -> str:
    return callable_operation_name(fn, default="")


def cached_operation_name(fn, default="") -> str:
    try:
        name = _cached_operation_name(fn)
    except TypeError:  # unhashable callable
        return callable_operation_name(fn, default=default)
    return name or default


def control_flow_predicate(names, *, module_prefix=None,
                           status_attrs=("status_code",)):
    """Build a ``span_event_scope`` predicate: True when the exception is one
    of the named framework HTTP-response exceptions carrying a sub-500 status
    — normal control flow (``abort(404)``, ``raise HTTPNotFound()``), not a
    failure. Matching rules and the meaning of a status of 0 are documented on
    :func:`http_exception_status`."""
    def _is_control_flow(exc) -> bool:
        status = http_exception_status(
            exc, names, module_prefix=module_prefix, status_attrs=status_attrs)
        return status is not None and status < 500
    return _is_control_flow


class span_event_scope:
    """Record ordinary exceptions on ``event`` and always end it on exit.

    A plain ``__enter__``/``__exit__`` class rather than a
    ``@contextmanager``: this scope sits on nearly every sampled operation, and
    the generator + _GeneratorContextManager pair the decorator allocates per
    call is pure overhead here.

    ``is_control_flow`` is an optional predicate called with the in-flight
    exception; when it returns True the exception is treated as framework
    control flow (a sub-500 HTTP response, e.g. ``flask.abort(404)`` /
    ``raise HTTPError(404)``) and is NOT recorded as a span-event error — the
    response status is captured separately by the root span. Default ``None``
    preserves the record-everything behavior every non-view caller (DB drivers,
    HTTP clients, brokers) relies on.
    """

    __slots__ = ("_event", "_is_control_flow")

    def __init__(self, event, is_control_flow=None) -> None:
        self._event = event
        self._is_control_flow = is_control_flow

    def __enter__(self):
        return self._event

    def __exit__(self, exc_type, exc_val, tb):
        try:
            if exc_val is not None and not self._control_flow(exc_val):
                record_exception_on_event(self._event, exc_val)
        finally:
            self._event.end()
        return False

    def _control_flow(self, exc_val) -> bool:
        """True when a classifier is set and reports ``exc_val`` as control
        flow. Never raises — a broken classifier must not mask the in-flight
        exception or skip ``end()``."""
        pred = self._is_control_flow
        if pred is None:
            return False
        try:
            return bool(pred(exc_val))
        except Exception:  # noqa: BLE001
            _log.debug("span_event_scope control-flow classifier raised",
                       exc_info=True)
            return False
