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

"""confluent-kafka instrumentation — producer + consumer.

``Producer`` and ``Consumer`` are librdkafka C extension types. wrapt can wrap
their methods, but two things follow from the C layer:

- ``produce`` is fire-and-forget and the message is immutable on return, so
  trace headers go into the ``headers`` kwarg before the call; the span event
  covers the queueing, not librdkafka's later network IO.
- ``SerializingProducer`` / ``DeserializingConsumer`` subclass ``cimpl.*``
  directly rather than the ``confluent_kafka.*`` names this module rebinds, so
  they are patched as their own concrete classes. Without that their
  ``produce`` / ``poll`` bypass instrumentation entirely.

See ``README.md`` for the span shapes (``poll`` holds its span, ``consume``
stays delivery-only).
"""

from __future__ import annotations

import functools
from typing import Optional

from ..._log import get_logger
from ...agent import get_agent
from ...context import current_span
from ...errors import safe_try
from ...instrumentor import BaseInstrumentor
from .._kafka import (
    advance_consume_scope as _advance_consume_scope,
    client_broker,
    close_consumer_scope as _close_consumer_scope,
    first_broker as _first_broker,
    open_consume_span,
    open_producer_event as _open_producer_event,
    trace_consumed_batch as _trace_consumed_batch,
)
from .._util import no_current_span, span_event_scope

_log = get_logger("confluent_kafka")
_OPERATION_PRODUCER_PRODUCE = "confluent_kafka.Producer.produce"


class ConfluentKafkaInstrumentor(BaseInstrumentor):
    def __init__(self) -> None:
        super().__init__()
        self._class_patches: list = []

    def _instrument(self) -> None:
        # ``cimpl.Producer``/``cimpl.Consumer`` are immutable C-extension types, so
        # setattr on them raises ``TypeError: cannot set 'produce' attribute``.
        # Subclass in pure Python and rebind the module-level names instead; the
        # subclass stays transparent to ``isinstance`` via ``__mro__``.
        try:
            import confluent_kafka  # type: ignore[import-not-found]
        except Exception:  # noqa: BLE001
            return
        producer_patch = _patch_class(confluent_kafka, "Producer", {
            "produce": (_producer_produce_wrapper, no_current_span),
        })
        consumer_patch = _patch_class(confluent_kafka, "Consumer", {
            # poll/consume may own a scope from the previous record. They must
            # enter the wrapper even when the agent was disabled in between so
            # that scope is released before the next fetch.
            "poll": (_consumer_poll_wrapper, None),
            "consume": (_consumer_consume_wrapper, None),
            # Closes the last held record scope; without it that span would
            # only end when the process does. No precheck — it must run even
            # with the agent disabled mid-run.
            "close": (_consumer_close_wrapper, None),
        })
        # The schema-registry APIs confluent documents for production. They subclass
        # the ORIGINAL ``cimpl.*`` types (bound at package import), so the rebinding
        # above never reaches them — their ``produce``/``poll`` call the C base via
        # ``super()``. Patch the concrete classes too; inheriting from ``cimpl.*``
        # rather than our subclasses means each call still routes through exactly one
        # wrapper. ``SerializingProducer.produce`` drops the ``callback`` slot and
        # swaps key/value, moving ``headers`` one slot earlier (see
        # ``_SERIALIZING_PRODUCE_HEADERS_POS``); ``DeserializingConsumer`` implements
        # only ``poll``.
        serializing_producer_patch = _patch_class(
            confluent_kafka, "SerializingProducer", {
                "produce": (_serializing_producer_produce_wrapper, no_current_span),
            })
        deserializing_consumer_patch = _patch_class(
            confluent_kafka, "DeserializingConsumer", {
                "poll": (_consumer_poll_wrapper, None),
                "close": (_consumer_close_wrapper, None),
            })
        for patch in (producer_patch, consumer_patch,
                      serializing_producer_patch, deserializing_consumer_patch):
            if patch is not None:
                self._class_patches.append(patch)

    def _uninstrument(self) -> None:
        patches, self._class_patches = self._class_patches, []
        for module, attr, original, patched in reversed(patches):
            try:
                if getattr(module, attr, None) is patched:
                    setattr(module, attr, original)
            except Exception:  # noqa: BLE001
                _log.debug("failed to restore %s.%s", module.__name__, attr,
                           exc_info=True)


def _patch_class(module, attr: str, method_wrappers: dict):
    """Replace ``module.attr`` with a Python subclass whose listed methods
    are wrapped through the ``wrapt``-style ``(wrapped, instance, args,
    kwargs) -> result`` callables.

    Each value is a ``(wrapper, precheck)`` pair; ``precheck()`` returns
    ``True`` when tracing is definitely off for that call (see ``_make_method``).
    On success, returns the ownership tuple used to restore the module slot.
    """
    orig = getattr(module, attr, None)
    if orig is None or getattr(orig, "_pinpoint_patched", False):
        return None

    # __module__/__qualname__ copied from the original: without them the
    # replacement reports this package, so anything introspecting (or pickling
    # a reference to) confluent_kafka.Consumer sees a pinpoint class. The
    # generated __init__ still has no real signature — these are C types whose
    # __init__ takes a config dict, so nothing here reads it.
    body: dict = {
        "_pinpoint_patched": True,
        "__init__": _make_init(orig),
        "__module__": getattr(orig, "__module__", module.__name__),
        "__qualname__": getattr(orig, "__qualname__", attr),
    }
    for name, (wrapper, precheck) in method_wrappers.items():
        orig_method = getattr(orig, name, None)
        if orig_method is None:
            continue
        body[name] = _make_method(orig_method, wrapper, precheck)

    try:
        subclass = type(attr, (orig,), body)
    except Exception:  # noqa: BLE001
        _log.debug("failed to subclass %s.%s", module.__name__, attr,
                   exc_info=True)
        return None
    try:
        setattr(module, attr, subclass)
    except Exception:  # noqa: BLE001
        _log.debug("failed to patch %s.%s", module.__name__, attr,
                   exc_info=True)
        return None
    return module, attr, orig, subclass


# The only config keys _bootstrap_server reads. _make_init stashes just these:
# a librdkafka config routinely carries sasl.password / ssl.key.password /
# sasl.oauthbearer.client.secret, and copying the dict whole would pin those
# secrets on the client for the life of the process to recover a hostname.
_BROKER_CONFIG_KEYS = ("bootstrap.servers", "metadata.broker.list")


def _make_init(orig_cls):
    """Wrap the C-extension ``__init__`` to stash the broker address on the
    instance. ``Producer(conf)`` / ``Consumer(conf)`` accept the config as a
    single positional dict (or as ``conf=`` kwarg); librdkafka stores it
    internally but never re-exposes it. Stashing the ``_BROKER_CONFIG_KEYS``
    entries on ``_pinpoint_config`` lets ``_bootstrap_server`` recover
    ``bootstrap.servers`` later without retaining the rest of the config."""
    orig_init = orig_cls.__init__

    def _init(self, *args, **kwargs):
        cfg = None
        if args and isinstance(args[0], dict):
            cfg = args[0]
        elif isinstance(kwargs.get("conf"), dict):
            cfg = kwargs["conf"]
        if cfg is not None:
            try:
                self._pinpoint_config = {
                    key: cfg[key] for key in _BROKER_CONFIG_KEYS if key in cfg
                }
            except Exception:  # noqa: BLE001
                pass
        orig_init(self, *args, **kwargs)

    return _init


def _make_method(orig_method, wrapper, precheck):
    """Build a Python method that funnels through the wrapt-style wrapper.

    ``safe_wrapper`` is applied so a misbehaving wrapper still falls back
    to the unwrapped C method instead of crashing the user's call.

    ``produce`` is fire-and-forget (thousands/sec) and ``poll`` runs in a
    tight loop, so the no-tracing case is the hot one. ``precheck()`` detects
    it up front and calls the raw C method directly, skipping the safe_wrapper
    machinery (its per-call state list + sentinel closure + extra frames).
    On the traced path, ``orig_method`` is bound to ``self`` via the descriptor
    protocol — one builtin bound-method allocation, no per-call closure cell.

    ``precheck=None`` is for a method that must always run its wrapper
    (``close``, which has a held span scope to release even with the agent
    disabled); it gets its own branch-free variant so the hot methods never
    test a precheck that cannot exist.
    """
    from .._util import safe_wrapper

    safe = safe_wrapper(wrapper)

    if precheck is None:
        def _bound(self, *args, **kwargs):
            return safe(orig_method.__get__(self, type(self)), self,
                        args, kwargs)
    else:
        def _bound(self, *args, **kwargs):
            if precheck():
                return orig_method(self, *args, **kwargs)
            return safe(orig_method.__get__(self, type(self)), self,
                        args, kwargs)

    _bound.__name__ = getattr(orig_method, "__name__", "method")
    return _bound


# ---------------------------------------------------------------- producer

_PRODUCE_HEADERS_POS = 7  # index of ``headers`` in cimpl.Producer.produce(
                         # topic, value, key, partition, callback, on_delivery,
                         # timestamp, headers). The C signature exposes ``callback``
                         # (4) and its alias ``on_delivery`` (5) as distinct slots, so
                         # ``headers`` is idx 7 — one past the docstring's 7-name
                         # shape. Verified against confluent-kafka 2.14.0.

_SERIALIZING_PRODUCE_HEADERS_POS = 6  # index of ``headers`` in
                         # SerializingProducer.produce(topic, key, value, partition,
                         # on_delivery, timestamp, headers): the override drops the C
                         # ``callback`` slot, so ``headers`` sits one slot earlier.
                         # ``topic`` is still idx 0. Verified against 2.14.0.


def _produce_wrapper(wrapped, instance, args, kwargs, *, headers_pos: int):
    span = current_span()
    if span is None:
        return wrapped(*args, **kwargs)

    event, new_args, new_kwargs, sampled = _open_producer_event(
        span, _OPERATION_PRODUCER_PRODUCE, args, kwargs, headers_pos,
        instance, _bootstrap_server,
    )
    if not sampled:
        return wrapped(*new_args, **new_kwargs)

    with span_event_scope(event):
        return wrapped(*new_args, **new_kwargs)


# The two produce surfaces differ only in the ``headers`` slot. Both go through
# ``safe_wrapper``, which logs ``fn.__qualname__`` on failure and partials have
# none — so set it (same pattern as elasticsearch's v7 transport wrapper).
# ``SerializingProducer.produce`` is injected on this outer call, before
# serialization; the override forwards ``headers`` to the C base, so the trace
# headers still reach the wire.
_producer_produce_wrapper = functools.partial(
    _produce_wrapper, headers_pos=_PRODUCE_HEADERS_POS,
)
_producer_produce_wrapper.__qualname__ = "_producer_produce_wrapper"
_serializing_producer_produce_wrapper = functools.partial(
    _produce_wrapper, headers_pos=_SERIALIZING_PRODUCE_HEADERS_POS,
)
_serializing_producer_produce_wrapper.__qualname__ = (
    "_serializing_producer_produce_wrapper"
)


def _bootstrap_server(client) -> str:
    """Return the *first* configured broker address.

    ``Producer`` and ``Consumer`` accept their config dict at construction
    time but don't expose it via a stable attribute. We stash the broker keys
    on ``_pinpoint_config`` from our subclass ``__init__`` (see ``_make_init``);
    third-party wrappers may also expose the *whole* config under common names,
    so those are read but never retained. librdkafka accepts
    ``bootstrap.servers`` as a comma-separated string — we take the first host
    to keep Pinpoint's endpoint label readable."""
    for attr in ("_pinpoint_config", "_config", "config", "_conf"):
        cfg = getattr(client, attr, None)
        if isinstance(cfg, dict):
            for key in _BROKER_CONFIG_KEYS:
                servers = cfg.get(key)
                if servers:
                    return _first_broker(servers)
    return ""


# ---------------------------------------------------------------- consumer

def _consumer_poll_wrapper(wrapped, instance, args, kwargs):
    """``Consumer.poll(timeout)`` returns a single Message (or None).

    Polling again means the caller is done with the message the previous poll
    handed them, so the previous scope closes here — before the call, so the
    span never covers poll's own blocking wait."""
    _close_consumer_scope(instance)
    message = wrapped(*args, **kwargs)
    agent = get_agent()
    if agent is None or not agent.enabled or message is None:
        return message
    if _message_has_error(message):
        return message
    _advance_consume_scope(
        instance, agent, message,
        lambda: client_broker(instance, _bootstrap_server),
        extract=_message_fields,
    )
    return message


def _consumer_close_wrapper(wrapped, instance, args, kwargs):
    _close_consumer_scope(instance)
    return wrapped(*args, **kwargs)


def _consumer_consume_wrapper(wrapped, instance, args, kwargs):
    """``Consumer.consume(num_messages=1, timeout=-1)`` returns a list of
    Message objects."""
    # Otherwise the batch's delivery spans would nest inside whatever record
    # scope a previous poll left open.
    _close_consumer_scope(instance)
    messages = wrapped(*args, **kwargs)
    agent = get_agent()
    if agent is None or not agent.enabled or not messages:
        return messages

    # Filter once because Message.error() is a C-extension call. Each valid
    # message still needs its own span: messages in one batch may carry
    # unrelated producer trace contexts.
    valid_messages = [
        message for message in messages if not _message_has_error(message)
    ]
    if not valid_messages:
        return messages
    _trace_consumed_batch(
        agent, valid_messages, lambda: client_broker(instance, _bootstrap_server),
        trace_record=_open_consume_span,
    )
    return messages


def _message_has_error(message) -> bool:
    """Confluent-kafka surfaces partition EOFs, errors, etc. via
    ``message.error()``. We skip those so we don't spam empty spans."""
    err_fn = getattr(message, "error", None)
    try:
        return err_fn() is not None if callable(err_fn) else False
    except Exception:  # noqa: BLE001
        return False


@safe_try
def _open_consume_span(
    agent,
    message,
    broker,
    batch_size: Optional[int] = None,
) -> None:
    open_consume_span(agent, *_message_fields(message), broker, batch_size)


def _message_fields(message):
    """``(topic, partition, offset, headers)`` off a confluent Message, whose
    fields are methods rather than attributes."""
    return (
        _safe_call(message, "topic", "") or "",
        _safe_call(message, "partition", None),
        _safe_call(message, "offset", None),
        _safe_call(message, "headers", None) or (),
    )


def _safe_call(obj, attr, default):
    """confluent-kafka Message exposes topic/partition/offset/headers as
    *methods* (C extension), not attributes. Some duck-typed fakes use
    plain attrs — accept either."""
    fn = getattr(obj, attr, default)
    if callable(fn):
        try:
            return fn()
        except Exception:  # noqa: BLE001
            return default
    return fn


def instrument() -> None:
    ConfluentKafkaInstrumentor().instrument()
