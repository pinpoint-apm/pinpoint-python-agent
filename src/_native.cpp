// pinpoint-python-agent
// Copyright (c) 2026-present NAVER Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// pybind11 binding for the embedded pinpoint-cpp-agent. Mostly mirrors
// `include/pinpoint/tracer.h` so the pure-Python layer stays thin; a few hot
// helpers return extra metadata to save a round trip.
//
// Two rules hold throughout:
// - Every call that can block on an agent mutex (span admission, the API cache,
//   prepareSql, a gRPC meta enqueue) releases the GIL: holding it across a
//   contended lock would stall every Python thread.
// - Marshalling runs first, GIL held — scalars extracted, utf8 views borrowed
//   from each str's cached buffer (utf8_view) — and the result is applied with
//   the GIL released. The caller's argument tuples keep every referenced object
//   alive for the whole call, so the views stay valid. The same holds for
//   std::string_view *arguments* and no copy is made: pybind11's caster
//   borrows the str's cached UTF-8 buffer (PyUnicode_AsUTF8AndSize), that
//   buffer is immutable and owned by the str, and the str is pinned by the
//   call's argument tuple until the binding returns. Python always passes str
//   here; a bytearray would be the one type whose buffer another thread could
//   resize under a released GIL, and none of these entry points takes one.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <pinpoint/tracer.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace py = pybind11;
namespace pp = pinpoint;

namespace {

// Native log callback bridge. The callback is invoked inline on the thread that
// emitted the line (including the agent's gRPC workers), so it must never
// acquire the GIL or block on a lock. This bounded MPSC queue uses only atomics
// plus bounded string copies on that path. Python drains it later, off that
// thread.
class NativeLogQueue {
public:
    static constexpr size_t kDefaultCapacity = 1024;
    static constexpr size_t kMinCapacity = 2;
    static constexpr size_t kMaxCapacity = 4096;
    static constexpr size_t kMaxMessageBytes = 4 * 1024;
    static constexpr size_t kMaxQueuedBytes = 4 * 1024 * 1024;
    static_assert(std::atomic<size_t>::is_always_lock_free,
                  "native log queue positions must be lock-free");
    static_assert(std::atomic<uint64_t>::is_always_lock_free,
                  "native log drop counter must be lock-free");
    static_assert(std::atomic<bool>::is_always_lock_free,
                  "native log activation flag must be lock-free");

    explicit NativeLogQueue(size_t requested_capacity)
        : capacity_(normalize_capacity(requested_capacity)),
          cells_(std::make_unique<Cell[]>(capacity_)) {
        for (size_t i = 0; i < capacity_; ++i) {
            cells_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    NativeLogQueue(const NativeLogQueue&) = delete;
    NativeLogQueue& operator=(const NativeLogQueue&) = delete;

    static size_t normalize_capacity(size_t value) noexcept {
        if (value < kMinCapacity || value > kMaxCapacity) {
            return kDefaultCapacity;
        }
        return value;
    }

    void enqueue(const char* level, const char* message) noexcept {
        if (!active_.load(std::memory_order_acquire)) {
            return;
        }

        const char* safe_level = level == nullptr ? "warning" : level;
        const char* safe_message = message == nullptr ? "" : message;
        const size_t level_size = bounded_length(safe_level, 16);
        const size_t message_size = utf8_bounded_length(safe_message);
        const size_t bytes = level_size + message_size;

        if (!reserve_bytes(bytes)) {
            dropped_.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &cells_[pos % capacity_];
            const size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto difference = static_cast<std::intptr_t>(seq) -
                                    static_cast<std::intptr_t>(pos);
            if (difference == 0) {
                if (enqueue_pos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (difference < 0) {
                queued_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return;
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }

        cell->record.level_size = level_size;
        cell->record.message_size = message_size;
        cell->record.bytes = bytes;
        std::memcpy(cell->record.level.data(), safe_level, level_size);
        std::memcpy(cell->record.message.data(), safe_message, message_size);
        cell->sequence.store(pos + 1, std::memory_order_release);
    }

    std::vector<std::pair<std::string, std::string>> drain(size_t maximum) {
        maximum = std::min(maximum, capacity_);
        std::vector<std::pair<std::string, std::string>> records;
        records.reserve(maximum);
        while (records.size() < maximum) {
            Record record;
            if (!try_dequeue(record)) {
                break;
            }
            records.emplace_back(
                std::string(record.level.data(), record.level_size),
                std::string(record.message.data(), record.message_size));
        }
        return records;
    }

    uint64_t dropped() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }

    uint64_t take_dropped() noexcept {
        return dropped_.exchange(0, std::memory_order_relaxed);
    }

    void deactivate() noexcept {
        active_.store(false, std::memory_order_release);
    }

    bool active() const noexcept {
        return active_.load(std::memory_order_acquire);
    }

    size_t capacity() const noexcept { return capacity_; }

private:
    struct Record {
        std::array<char, 16> level{};
        std::array<char, kMaxMessageBytes> message{};
        size_t level_size{0};
        size_t message_size{0};
        size_t bytes{0};
    };

    struct Cell {
        std::atomic<size_t> sequence{0};
        Record record;
    };

    static size_t bounded_length(const char* value, size_t maximum) noexcept {
        return ::strnlen(value, maximum);
    }

    static size_t utf8_bounded_length(const char* value) noexcept {
        const size_t size = ::strnlen(value, kMaxMessageBytes + 1);
        if (size <= kMaxMessageBytes) {
            return size;
        }
        // Native log strings are valid UTF-8. If the byte cap lands in a
        // multibyte code point, exclude that entire code point.
        size_t end = kMaxMessageBytes;
        while (end > 0 &&
               (static_cast<unsigned char>(value[end]) & 0xc0U) == 0x80U) {
            --end;
        }
        return end;
    }

    bool reserve_bytes(size_t bytes) noexcept {
        if (bytes > kMaxQueuedBytes) {
            return false;
        }
        size_t current = queued_bytes_.load(std::memory_order_relaxed);
        while (current <= kMaxQueuedBytes - bytes) {
            if (queued_bytes_.compare_exchange_weak(
                    current, current + bytes, std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    bool try_dequeue(Record& output) noexcept {
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);
        Cell* cell = nullptr;
        for (;;) {
            cell = &cells_[pos % capacity_];
            const size_t seq = cell->sequence.load(std::memory_order_acquire);
            const auto difference = static_cast<std::intptr_t>(seq) -
                                    static_cast<std::intptr_t>(pos + 1);
            if (difference == 0) {
                if (dequeue_pos_.compare_exchange_weak(
                        pos, pos + 1, std::memory_order_relaxed)) {
                    break;
                }
            } else if (difference < 0) {
                return false;
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }

        output = cell->record;
        const size_t bytes = output.bytes;
        cell->record.level_size = 0;
        cell->record.message_size = 0;
        cell->record.bytes = 0;
        cell->sequence.store(pos + capacity_, std::memory_order_release);
        queued_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
        return true;
    }

    const size_t capacity_;
    std::unique_ptr<Cell[]> cells_;
    alignas(64) std::atomic<size_t> enqueue_pos_{0};
    alignas(64) std::atomic<size_t> dequeue_pos_{0};
    std::atomic<size_t> queued_bytes_{0};
    std::atomic<uint64_t> dropped_{0};
    std::atomic<bool> active_{true};
};

class NativeLogBridge {
public:
    explicit NativeLogBridge(size_t capacity)
        : queue_(std::make_shared<NativeLogQueue>(capacity)) {}

    std::shared_ptr<NativeLogQueue> queue() const noexcept { return queue_; }
    auto drain(size_t maximum) { return queue_->drain(maximum); }
    uint64_t dropped() const noexcept { return queue_->dropped(); }
    uint64_t take_dropped() noexcept { return queue_->take_dropped(); }
    void deactivate() noexcept { queue_->deactivate(); }
    bool active() const noexcept { return queue_->active(); }
    size_t capacity() const noexcept { return queue_->capacity(); }

private:
    std::shared_ptr<NativeLogQueue> queue_;
};

// Borrowed view of a str's cached UTF-8 buffer. Casting to std::string
// instead would have the native setter copy the text twice.
std::string_view utf8_view(const py::handle& h) {
    Py_ssize_t size = 0;
    const char* data = PyUnicode_AsUTF8AndSize(h.ptr(), &size);
    if (data == nullptr) {
        throw py::error_already_set();
    }
    return {data, static_cast<size_t>(size)};
}

// Stands in for a str the interpreter cannot encode. One visible bucket rather
// than per-event mojibake: the API cache is keyed on this text.
constexpr std::string_view kUnencodableField = "<unencodable>";

// utf8_view for a field whose loss must not cost the whole record. A str can
// fail to encode — a lone surrogate, which is what surrogateescape yields for a
// non-UTF-8 filename — and letting that propagate would drop the span event
// along with every annotation, SQL statement and error recorded on it, all of
// them still perfectly encodable. Report the field and carry on with
// `fallback`. A non-str is a different case and still throws: only our own
// tracer writes these fields, so it means a malformed record, not app data.
std::string_view utf8_view_or(const py::handle& h, std::string_view fallback) {
    if (!PyUnicode_Check(h.ptr())) {
        throw py::type_error("span event string field is not a str");
    }
    Py_ssize_t size = 0;
    const char* data = PyUnicode_AsUTF8AndSize(h.ptr(), &size);
    if (data == nullptr) {
        py::error_already_set unencodable;
        unencodable.discard_as_unraisable(
            "pinpoint: span event field not encodable to UTF-8, replaced");
        return fallback;
    }
    return {data, static_cast<size_t>(size)};
}

// Convert the SQL bind values tracer._snapshot_sql_binds buffered into the
// scalar variants the native agent accepts. The Python side already froze them:
// `args` is a str, a tuple of None/str/bool/int/float, or one such scalar, and
// every int fits int64/uint64 (anything else was str()-ed there). String binds
// stay borrowed views (see utf8_view) into objects the caller's annotation
// tuple keeps alive. An empty str means "no bind arguments" — the default for
// every traced query while bind capture is off — so that path builds nothing.
pp::SqlBindValue marshal_sql_bind(const py::handle& value) {
    if (value.is_none()) {
        return pp::SqlBindValue(nullptr);
    }
    if (PyUnicode_Check(value.ptr())) {
        return pp::SqlBindValue(utf8_view(value));
    }
    if (PyBool_Check(value.ptr())) {
        return pp::SqlBindValue(value.ptr() == Py_True);
    }
    if (PyLong_Check(value.ptr())) {
        int overflow = 0;
        const long long signed_value =
            PyLong_AsLongLongAndOverflow(value.ptr(), &overflow);
        if (overflow == 0 && !PyErr_Occurred()) {
            if (signed_value >= std::numeric_limits<int32_t>::min() &&
                signed_value <= std::numeric_limits<int32_t>::max()) {
                return pp::SqlBindValue(static_cast<int32_t>(signed_value));
            }
            if (signed_value >= 0 &&
                static_cast<unsigned long long>(signed_value) <=
                    std::numeric_limits<uint32_t>::max()) {
                return pp::SqlBindValue(static_cast<uint32_t>(signed_value));
            }
            return pp::SqlBindValue(static_cast<int64_t>(signed_value));
        }
        PyErr_Clear();
        const unsigned long long unsigned_value =
            PyLong_AsUnsignedLongLong(value.ptr());
        if (PyErr_Occurred()) {
            throw py::error_already_set();
        }
        return pp::SqlBindValue(static_cast<uint64_t>(unsigned_value));
    }
    if (PyFloat_Check(value.ptr())) {
        return pp::SqlBindValue(PyFloat_AsDouble(value.ptr()));
    }
    throw py::type_error("SQL bind value is not a frozen scalar");
}

std::vector<pp::SqlBindValue> marshal_sql_binds(const py::handle& args) {
    std::vector<pp::SqlBindValue> bind_values;
    if (PyUnicode_Check(args.ptr())) {
        if (py::len(args) != 0) {
            bind_values.emplace_back(utf8_view(args));
        }
    } else if (PyTuple_Check(args.ptr())) {
        auto values = py::reinterpret_borrow<py::tuple>(args);
        bind_values.reserve(values.size());
        for (py::handle value : values) {
            bind_values.push_back(marshal_sql_bind(value));
        }
    } else {
        bind_values.push_back(marshal_sql_bind(args));
    }
    return bind_values;
}

// Build the frame vector for the SetError frame-list overload from the
// Python-dumped list of (module, function, file, line) tuples. Views point
// into each str's cached UTF-8 buffer — no per-frame std::string copy; the
// tuples (and their strs) are kept alive by the caller-held sequence.
std::vector<pp::CallStackFrame> to_callstack_frames(const py::sequence& items) {
    std::vector<pp::CallStackFrame> frames;
    frames.reserve(py::len(items));
    for (py::handle handle : items) {
        auto frame = py::reinterpret_borrow<py::tuple>(handle);
        // Braced init is sequenced left-to-right, so each view is built before
        // the next; all three stay owned by the caller-held tuple.
        frames.push_back(pp::CallStackFrame{
            utf8_view(frame[0]), utf8_view(frame[1]), utf8_view(frame[2]),
            frame[3].cast<int>()});
    }
    return frames;
}

// One wrapper-buffered annotation, marshalled for apply_marshalled. The tag
// selects the ``SetAnnotation`` overload and must stay in sync with the
// ``_ANN_*`` constants in tracer.py:
//   tag 0 -> SetAnnotation(key, int32)
//   tag 1 -> SetAnnotation(key, str)
//   tag 2 -> SetAnnotation(key, str, str)
//   tag 3 -> SetAnnotation(key, int64)
//   tag 4 -> SetSqlQuery(sql, binds) — item is (tag, sql, args); SpanEvent only
//   tag 5 -> SetError — item is (tag, message) or (tag, name, message[, frames[, causes]])
//            where frames is a list of (module, function, file, line) tuples
//            dumped Python-side and causes a list of (name, message, frames)
//            for the exception chain; both forms exist on SpanEvent only
//   tag 6 -> SetAnnotation(key, int64, int32, int32, int32, int32, str) — the
//            composite proxy-header payload; Span only
//   tag 7 -> SetIgnoredError — tag-5 shape with arity >= 2: recorded like
//            SetError but never marks the transaction failed (Python matched
//            a subclass/cause ignore rule)
struct MarshalledAnnotation {
    int tag = 0;
    int32_t key = 0;
    int64_t num = 0;                      // tags 0 (int32 range), 3, 6
    int32_t composite[4] = {0, 0, 0, 0};  // tag 6 int/int/byte/byte
    std::string_view s1, s2;              // tags 1, 2, 5, 6; tag 4 SQL text
    int error_arity = 0;                  // tag 5: 1, 2, or 3 (with frames)
    std::vector<pp::CallStackFrame> frames;          // tag 5 frames form
    std::vector<pp::ExceptionChainEntry> causes;     // tag 5 chain form
    std::vector<pp::SqlBindValue> binds;             // tag 4
};

// Marshal a buffer of annotation tuples (GIL held). One malformed or
// out-of-range value must not abort the finalize and leak the span, so each
// item marshals under its own try/catch: a bad annotation is dropped and the
// rest always survive. A Python-level failure is reported through
// sys.unraisablehook so it stays visible; a pybind cast failure (an
// out-of-range key or value) carries no Python error to report and is dropped
// silently — that is the one gap to know about when an annotation goes
// missing.
std::vector<MarshalledAnnotation> marshal_annotations(const py::sequence& items) {
    std::vector<MarshalledAnnotation> result;
    result.reserve(py::len(items));
    for (const auto& handle : items) {
        try {
            auto item = py::reinterpret_borrow<py::tuple>(handle);
            MarshalledAnnotation a;
            a.tag = item[0].cast<int>();
            switch (a.tag) {
                case 0:
                    a.key = item[1].cast<int32_t>();
                    // int32 cast raises on overflow, so an out-of-range
                    // value drops just this annotation.
                    a.num = item[2].cast<int32_t>();
                    break;
                case 1:
                    a.key = item[1].cast<int32_t>();
                    a.s1 = utf8_view(item[2]);
                    break;
                case 2:
                    a.key = item[1].cast<int32_t>();
                    a.s1 = utf8_view(item[2]);
                    a.s2 = utf8_view(item[3]);
                    break;
                case 3:
                    a.key = item[1].cast<int32_t>();
                    a.num = item[2].cast<int64_t>();
                    break;
                case 4:
                    // (tag, sql, args): item[1] is the SQL text, not an int key.
                    a.s1 = utf8_view(item[1]);
                    a.binds = marshal_sql_binds(item[2]);
                    break;
                case 5:
                case 7: {
                    // (tag, message) or (tag, name, message[, frames]):
                    // item[1]/item[2] are strings, not an int key.
                    const size_t n = item.size();
                    a.s1 = utf8_view(item[1]);
                    a.error_arity = 1;
                    if (n >= 3) {
                        a.s2 = utf8_view(item[2]);
                        a.error_arity = 2;
                    }
                    if (n >= 4) {
                        a.frames = to_callstack_frames(py::sequence(item[3]));
                        a.error_arity = 3;
                    }
                    if (n >= 5 && !item[4].is_none()) {
                        // Views borrow from the strs inside the buffered
                        // tuple, which the caller's argument keeps alive for
                        // the whole call.
                        for (py::handle h : py::sequence(item[4])) {
                            auto cause = py::reinterpret_borrow<py::tuple>(h);
                            a.causes.push_back(pp::ExceptionChainEntry{
                                utf8_view(cause[0]), utf8_view(cause[1]),
                                to_callstack_frames(py::sequence(cause[2]))});
                        }
                    }
                    break;
                }
                case 6:
                    a.key = item[1].cast<int32_t>();
                    a.num = item[2].cast<int64_t>();
                    a.composite[0] = item[3].cast<int32_t>();
                    a.composite[1] = item[4].cast<int32_t>();
                    a.composite[2] = item[5].cast<int32_t>();
                    a.composite[3] = item[6].cast<int32_t>();
                    a.s1 = utf8_view(item[7]);
                    break;
                default:
                    continue;  // unknown tag: dropped silently
            }
            result.push_back(std::move(a));
        } catch (py::error_already_set& e) {
            // Report as unraisable and drop just this annotation: that logs the
            // failure and clears the error indicator before the finalize.
            e.discard_as_unraisable("pinpoint: skipped malformed annotation");
        } catch (const std::exception&) {
            // pybind11 cast failures surface as std::runtime_error, not
            // error_already_set; swallow those too. Finalize must not abort.
            PyErr_Clear();
        }
    }
    return result;
}

// An error verdict whose profiling detail was discarded by the Python
// wrapper. Unlike tag-5 annotations this never calls Span::SetError and thus
// cannot create root error fields, event exceptionInfo or exception metadata.
struct MarshalledErrorVerdict {
    std::string_view name;
    std::string_view message;
};

// Marshal under the GIL with per-item containment. A malformed verdict must
// not prevent the other verdicts, event replay, URL stat, or EndSpan.
std::vector<MarshalledErrorVerdict> marshal_error_verdicts(
        const py::sequence& items) {
    std::vector<MarshalledErrorVerdict> result;
    result.reserve(py::len(items));
    for (const auto& handle : items) {
        try {
            auto item = py::reinterpret_borrow<py::tuple>(handle);
            result.push_back({utf8_view(item[0]), utf8_view(item[1])});
        } catch (py::error_already_set& e) {
            e.discard_as_unraisable(
                "pinpoint: skipped malformed error verdict");
        } catch (const std::exception&) {
            PyErr_Clear();
        }
    }
    return result;
}

void apply_error_verdicts(
        pp::Span& span,
        const std::vector<MarshalledErrorVerdict>& errors) {
    for (const auto& error : errors) {
        try {
            span.MarkError(error.name, error.message);
        } catch (...) {
            // Native policy evaluation is isolated per verdict just like
            // apply_marshalled isolates ordinary annotations.
        }
    }
}

// Apply marshalled annotations onto a native span/span event in one shot,
// during finalize just before EndSpan()/EndEvent(). GIL released (SetSqlQuery
// and SetError take the agent's cache locks and may enqueue gRPC meta), so no
// C-API runs from here on; a native failure drops the one annotation, never
// the finalize.
template <typename T>
void apply_marshalled(T& holder, const std::vector<MarshalledAnnotation>& anns) {
    for (const auto& a : anns) {
        try {
            switch (a.tag) {
                case 0:
                    holder.SetAnnotation(a.key, static_cast<int32_t>(a.num));
                    break;
                case 1:
                    holder.SetAnnotation(a.key, a.s1);
                    break;
                case 2:
                    holder.SetAnnotation(a.key, a.s1, a.s2);
                    break;
                case 3:
                    holder.SetAnnotation(a.key, a.num);
                    break;
                case 4:
                    // Span never buffers this tag, so the Span instantiation
                    // compiles it out.
                    if constexpr (std::is_same_v<T, pp::SpanEvent>) {
                        holder.SetSqlQuery(a.s1, a.binds);
                    }
                    break;
                case 5:
                    if (a.error_arity == 1) {
                        holder.SetError(a.s1);
                    } else if (a.error_arity == 2) {
                        holder.SetError(a.s1, a.s2);
                    } else if constexpr (std::is_same_v<T, pp::SpanEvent>) {
                        // The frames form exists on SpanEvent only; causes
                        // join the same chain with depth 1, 2, ...
                        holder.SetError(a.s1, a.s2, a.frames, a.causes);
                    } else {
                        // Span with frames: only SpanEvent.set_error buffers
                        // them, so this is unreachable — record the error
                        // without them rather than drop it.
                        holder.SetError(a.s1, a.s2);
                    }
                    break;
                case 7:
                    if constexpr (std::is_same_v<T, pp::SpanEvent>) {
                        holder.SetIgnoredError(a.s1, a.s2, a.frames, a.causes);
                    } else {
                        holder.SetIgnoredError(a.s1, a.s2);
                    }
                    break;
                case 6:
                    // Composite long/int/int/byte/byte/string annotation (the
                    // proxy-header payload). SpanEvent has no such overload, so
                    // the SpanEvent instantiation compiles it out.
                    if constexpr (std::is_same_v<T, pp::Span>) {
                        holder.SetAnnotation(a.key, a.num, a.composite[0],
                                             a.composite[1], a.composite[2],
                                             a.composite[3], a.s1);
                    }
                    break;
                default:
                    break;
            }
        } catch (...) {
            // GIL released: nothing Python-side to report; drop this one so
            // the rest and the finalize still run.
        }
    }
}

// One wrapper-buffered, already-completed span event, marshalled like
// MarshalledAnnotation. Layout per Python item (built by SpanEvent._finalize
// in tracer.py):
//   (sequence, depth, start_ms, end_ms, service_type, operation,
//    destination, end_point, next_span_id, async_id, annotations)
struct MarshalledSpanEvent {
    int32_t sequence = 0;
    int32_t depth = 0;
    int64_t start_ms = 0;
    int64_t end_ms = 0;
    int32_t service_type = 0;
    std::string_view operation, destination, end_point;
    int64_t next_span_id = 0;
    int32_t async_id = 0;
    std::vector<MarshalledAnnotation> annotations;
};

// Same per-item containment as marshal_annotations: one malformed event is
// dropped (reported via sys.unraisablehook), the rest and the span finalize
// always run.
std::vector<MarshalledSpanEvent> marshal_span_events(const py::sequence& items) {
    std::vector<MarshalledSpanEvent> result;
    result.reserve(py::len(items));
    for (const auto& handle : items) {
        try {
            auto item = py::reinterpret_borrow<py::tuple>(handle);
            MarshalledSpanEvent ev;
            ev.sequence = item[0].cast<int32_t>();
            ev.depth = item[1].cast<int32_t>();
            ev.start_ms = item[2].cast<int64_t>();
            ev.end_ms = item[3].cast<int64_t>();
            ev.service_type = item[4].cast<int32_t>();
            // Per-field degradation: an unencodable name must not take the
            // event's annotations with it. destination/end_point fall back to
            // empty, which the replay already reads as "not set".
            ev.operation = utf8_view_or(item[5], kUnencodableField);
            ev.destination = utf8_view_or(item[6], {});
            ev.end_point = utf8_view_or(item[7], {});
            ev.next_span_id = item[8].cast<int64_t>();
            ev.async_id = item[9].cast<int32_t>();
            ev.annotations = marshal_annotations(py::sequence(item[10]));
            result.push_back(std::move(ev));
        } catch (py::error_already_set& e) {
            e.discard_as_unraisable("pinpoint: skipped malformed span event");
        } catch (const std::exception&) {
            PyErr_Clear();
        }
    }
    return result;
}

// Replay the marshalled events onto the native span in one pass, just before
// EndSpan(). Python owns event creation, the event stack, sequence/depth and
// both timestamps; native sees each event only here (GIL released:
// RecordSpanEvent resolves the API-cache id per event). An event that fails
// between RecordSpanEvent and EndEvent stays on the native stack and is
// drained by EndSpan with its preset end time.
void replay_marshalled_events(pp::Span& span,
                              const std::vector<MarshalledSpanEvent>& events) {
    for (const auto& ev : events) {
        try {
            pp::SpanEventPtr event = span.RecordSpanEvent(
                ev.operation, ev.service_type, ev.sequence, ev.depth,
                ev.start_ms, ev.end_ms, ev.async_id);
            // Unreachable against SpanImpl, which returns the shared noop
            // event on every failure path — but SpanEventPtr is a raw pointer
            // and RecordSpanEvent is virtual, so this guard is the difference
            // between skipping an event and dereferencing null.
            if (event == nullptr) {
                continue;
            }
            if (!ev.destination.empty()) {
                event->SetDestination(ev.destination);
            }
            if (!ev.end_point.empty()) {
                event->SetEndPoint(ev.end_point);
            }
            // 0 means the wrapper never injected outbound context from this
            // event — Python generates the child span id at inject time.
            if (ev.next_span_id != 0) {
                event->SetNextSpanId(ev.next_span_id);
            }
            apply_marshalled(*event, ev.annotations);
            event->EndEvent();
        } catch (...) {
            // GIL released: drop this event; the rest and EndSpan still run.
        }
    }
}

// Span finalization: the terminal set-metadata-then-finalize sequence behind
// the Span end methods below.
//
// Empty/zero arguments mean "never set on the Python wrapper", so the native
// setter is skipped rather than overwriting state the native span already holds.
// The native setters do not guard: an unconditional call would wipe the context
// extracted from the upstream Pinpoint-Host header (acceptorHost/endPoint/
// remoteAddr), reset the span service type, and stamp a bogus "HTTP status: 0"
// annotation on every non-HTTP span. SetStatusCode also records an error flag, so
// it needs a real status; a URL stat without a pattern is meaningless.
//
// Every setter runs under its own try/catch, and so does EndSpan(): the
// wrapper has already latched _ended and dropped its handle, so a setter that
// throws (SetStatusCode and SetServiceType allocate outside the agent's
// CATCH_AND_LOG guard) would otherwise skip EndSpan() and lose the whole
// transaction — no retry exists Python-side. The GIL is released here, so a
// failure is dropped rather than reported; EndSpan() failing is the one case
// left to propagate, as the wrapper logs that one.
template <typename Fn>
void apply_quietly(Fn&& fn) noexcept {
    try {
        fn();
    } catch (...) {
    }
}

void finalize_span_url_stat(pp::Span& span, std::string_view url_pattern,
                            std::string_view method, int32_t status_code) {
    if (status_code != 0) {
        apply_quietly([&] { span.SetStatusCode(status_code); });
    }
    if (!url_pattern.empty()) {
        apply_quietly([&] { span.SetUrlStat(url_pattern, method, status_code); });
    }
    span.EndSpan();
}

py::tuple to_python_config(const pp::SpanConfigSnapshot& config) {
    // The header lists cast to Python lists (pybind11/stl.h); the Python side
    // reads this tuple positionally (see http_helper.py).
    return py::make_tuple(
        config.application_name,
        config.application_type,
        config.service_name,
        config.max_event_depth,
        config.max_event_sequence,
        config.http_server_headers[pp::HTTP_REQUEST],
        config.http_server_headers[pp::HTTP_RESPONSE],
        config.http_server_headers[pp::HTTP_COOKIE],
        config.http_client_headers[pp::HTTP_REQUEST],
        config.http_client_headers[pp::HTTP_RESPONSE],
        config.http_client_headers[pp::HTTP_COOKIE],
        config.revision,
        // Append-only ABI for positional Python consumers and external test
        // doubles: never insert or reorder fields after shipping an index.
        config.sql_trace_bind_value,
        config.http_server_proxy_user_header_names,
        config.enable_callstack_trace,
        config.http_client_record_url_query,
        config.http_server_record_request_param,
        config.http_server_real_ip_header,
        config.http_server_real_ip_empty_value);
}

} // namespace

PYBIND11_MODULE(_native, m) {
    m.doc() = "pinpoint-cpp-agent pybind11 binding";

    // Python re-declares the constants it needs (propagator.py, annotation.py,
    // service_type.py), so none of tracer.h's are re-exposed here. Header
    // readers are pure Python too (http_helper): trace context arrives as the
    // pre-extracted dict new_span takes, and header recording runs
    // interpreter-side.

    py::class_<NativeLogBridge>(m, "NativeLogBridge")
        .def(py::init<size_t>(), py::arg("capacity") =
                                      NativeLogQueue::kDefaultCapacity)
        .def("drain", &NativeLogBridge::drain,
             py::arg("max_records") = NativeLogQueue::kDefaultCapacity)
        .def("dropped", &NativeLogBridge::dropped)
        .def("take_dropped", &NativeLogBridge::take_dropped)
        .def("deactivate", &NativeLogBridge::deactivate)
        .def_property_readonly("active", &NativeLogBridge::active)
        .def_property_readonly("capacity", &NativeLogBridge::capacity)
        // Test seam for the callback algorithm itself. The GIL is released
        // before entering the exact enqueue path used by the native logger.
        .def("_enqueue_for_test",
             [](NativeLogBridge& self, std::string level,
                std::string message) {
                 auto queue = self.queue();
                 py::gil_scoped_release release;
                 queue->enqueue(level.c_str(), message.c_str());
             },
             py::arg("level"), py::arg("message"))
        // Joins while the caller still owns the GIL: this would deadlock if
        // enqueue ever tried to enter Python from the producer thread.
        .def("_enqueue_from_native_thread_for_test",
             [](NativeLogBridge& self, std::string level,
                std::string message) {
                 auto queue = self.queue();
                 std::thread producer([queue, level = std::move(level),
                                       message = std::move(message)] {
                     queue->enqueue(level.c_str(), message.c_str());
                 });
                 producer.join();
             },
             py::arg("level"), py::arg("message"))
        .def("_enqueue_many_for_test",
             [](NativeLogBridge& self, size_t producer_count,
                size_t records_per_producer) {
                 auto queue = self.queue();
                 std::vector<std::thread> producers;
                 producers.reserve(producer_count);
                 for (size_t producer = 0; producer < producer_count;
                      ++producer) {
                     producers.emplace_back([queue, producer,
                                             records_per_producer] {
                         const std::string prefix =
                             "producer-" + std::to_string(producer) + "-";
                         for (size_t record = 0;
                              record < records_per_producer; ++record) {
                             const std::string message =
                                 prefix + std::to_string(record);
                             queue->enqueue("info", message.c_str());
                         }
                     });
                 }
                 for (auto& producer : producers) {
                     producer.join();
                 }
             },
             py::arg("producer_count"), py::arg("records_per_producer"));

    m.attr("NATIVE_LOG_DEFAULT_QUEUE_SIZE") =
        py::int_(NativeLogQueue::kDefaultCapacity);
    m.attr("NATIVE_LOG_MIN_QUEUE_SIZE") =
        py::int_(NativeLogQueue::kMinCapacity);
    m.attr("NATIVE_LOG_MAX_QUEUE_SIZE") =
        py::int_(NativeLogQueue::kMaxCapacity);
    m.attr("NATIVE_LOG_MAX_MESSAGE_BYTES") =
        py::int_(NativeLogQueue::kMaxMessageBytes);
    m.attr("NATIVE_LOG_MAX_QUEUED_BYTES") =
        py::int_(NativeLogQueue::kMaxQueuedBytes);

    // ------------------------------------------------------------------
    // Span
    //
    // No SpanEvent binding: span events are created, positioned and timed by
    // the pure-Python wrappers and replayed in one batch through
    // end_span_with_data, so native SpanEvent handles never reach Python.
    // ------------------------------------------------------------------
    py::class_<pp::Span, pp::SpanPtr>(m, "Span")
        .def("get_config_snapshot",
             [](pp::Span& self) {
                 pp::SpanConfigSnapshot config;
                 {
                     py::gil_scoped_release release;
                     config = self.GetConfigSnapshot();
                 }
                 return to_python_config(config);
             })
        // Unsampled spans end here: status/url_stat only when the wrapper
        // cached one (see finalize_span_url_stat), else a plain EndSpan().
        .def("end_span",
             [](pp::Span& self, std::string_view url_pattern,
                std::string_view method, int32_t status_code,
                const py::sequence& error_verdicts) {
                 // url_pattern/method stay borrowed views across the release:
                 // safe per header rule two (str-backed, pinned by the call).
                 auto errors = marshal_error_verdicts(error_verdicts);
                 py::gil_scoped_release release;
                 apply_error_verdicts(self, errors);
                 finalize_span_url_stat(self, url_pattern, method, status_code);
             },
             py::arg("url_pattern") = "", py::arg("method") = "",
             py::arg("status_code") = 0,
             py::arg("error_verdicts") = py::tuple())
        .def("mark_error",
             [](pp::Span& self, std::string_view error_name,
                std::string_view error_message) {
                 // Borrowed views are safe across the release: header rule two.
                 py::gil_scoped_release release;
                 self.MarkError(error_name, error_message);
             },
             py::arg("error_name"), py::arg("error_message"))
        .def("new_async_span",
             [](pp::Span& self, std::string_view async_operation,
                int32_t async_id, int32_t async_sequence) {
                 // The async link ids are Python-managed (stamped on the
                 // parent event, flushed with the batch). NewAsyncSpan waits
                 // on the agent's API cache lock; the borrowed view is safe
                 // across the release (header rule two).
                 py::gil_scoped_release release;
                 return self.NewAsyncSpan(async_operation, async_id,
                                          async_sequence);
             },
             py::arg("async_operation"), py::arg("async_id"),
             py::arg("async_sequence"))
        .def("record_span_events",
             [](pp::Span& self, const py::sequence& span_events) {
                 // Mid-span replay of already-finished events (same record
                 // shape as end_span_with_data). Native chunks them once
                 // span.event_chunk_size finished events accumulate.
                 auto events = marshal_span_events(span_events);
                 py::gil_scoped_release release;
                 replay_marshalled_events(self, events);
             },
             py::arg("span_events"))
        .def("end_span_with_data",
             [](pp::Span& self, int32_t service_type,
                std::string_view remote_addr, std::string_view endpoint,
                std::string_view acceptor_host, int32_t status_code,
                std::string_view url_pattern, std::string_view method,
                const py::sequence& error_verdicts,
                const py::sequence& annotations,
                const py::sequence& span_events, bool logging) {
                 // Marshal under the GIL, replay and flush without it. The
                 // string_view arguments stay borrowed across the release:
                 // safe per header rule two.
                 auto events = marshal_span_events(span_events);
                 auto anns = marshal_annotations(annotations);
                 auto errors = marshal_error_verdicts(error_verdicts);
                 py::gil_scoped_release release;
                 replay_marshalled_events(self, events);
                 apply_error_verdicts(self, errors);
                 apply_marshalled(self, anns);
                 // Empty/zero means "never set on the wrapper" — see
                 // finalize_span_url_stat for why the setters are skipped.
                 // Each setter isolated so a throw cannot skip EndSpan()
                 // (see apply_quietly).
                 if (service_type != 0) {
                     apply_quietly([&] { self.SetServiceType(service_type); });
                 }
                 if (!remote_addr.empty()) {
                     apply_quietly([&] { self.SetRemoteAddress(remote_addr); });
                 }
                 if (!endpoint.empty()) {
                     apply_quietly([&] { self.SetEndPoint(endpoint); });
                 }
                 if (!acceptor_host.empty()) {
                     apply_quietly([&] { self.SetAcceptorHost(acceptor_host); });
                 }
                 // The wrapper wrote the trace/span ids into a log record
                 // itself, so only the flag crosses.
                 if (logging) {
                     apply_quietly([&] { self.SetLogging(); });
                 }
                 finalize_span_url_stat(self, url_pattern, method, status_code);
             },
             py::arg("service_type"), py::arg("remote_addr"),
             py::arg("endpoint"), py::arg("acceptor_host"),
             py::arg("status_code"), py::arg("url_pattern"), py::arg("method"),
             py::arg("error_verdicts"), py::arg("annotations"),
             py::arg("span_events"), py::arg("logging"));

    // ------------------------------------------------------------------
    // Agent
    // ------------------------------------------------------------------
    py::class_<pp::Agent, pp::AgentPtr>(m, "Agent")
        // Returns (span, sampled, trace_id, span_id, revision):
        // the identity and the config revision the span captured ride the one
        // creation call. trace_id is the wire form (agentId^startTime^sequence),
        // "" for a noop or unsampled span; revision is 0 for those. The wrapper
        // caches get_config_snapshot() and re-fetches it only when a sampled
        // span's revision differs — a hot reload happened.
        .def("new_span",
             [](pp::Agent& self, std::string_view op, std::string_view rpc,
                const std::map<std::string, std::string>& pinpoint_headers,
                std::string_view method) {
                 // NewSpan()'s addActiveSpan takes a mutex shared with the
                 // stats thread: create + sample without the GIL. The
                 // string_view arguments stay borrowed across the release
                 // (header rule two); pybind already materialized the headers
                 // dict into the map, so extraction runs on plain C++ strings.
                 pp::SpanPtr span;
                 bool sampled = false;
                 std::string trace_id;
                 int64_t span_id = 0;
                 int64_t revision = 0;
                 {
                     py::gil_scoped_release release;
                     span = self.NewSpan(op, rpc, method, pinpoint_headers);
                     if (span) {
                         sampled = span->IsSampled();
                         trace_id = span->GetTraceId();
                         span_id = span->GetSpanId();
                         revision = span->GetConfigRevision();
                     }
                 }
                 return py::make_tuple(std::move(span), sampled, trace_id,
                                       span_id, revision);
             },
             py::arg("operation"), py::arg("rpc_point"),
             py::arg("pinpoint_headers"), py::arg("method"))
        .def("get_config_snapshot",
             [](pp::Agent& self) {
                 // The agent's current resolved config generation, revision
                 // at index 11. Fetched once at startup; revision changes use
                 // Span::GetConfigSnapshot so a concurrent reload cannot race.
                 pp::SpanConfigSnapshot config;
                 {
                     py::gil_scoped_release release;
                     config = self.GetConfigSnapshot();
                 }
                 return to_python_config(config);
             })
        .def("enable", &pp::Agent::Enable)
        .def("shutdown", &pp::Agent::Shutdown,
             py::call_guard<py::gil_scoped_release>());

    // ------------------------------------------------------------------
    // Free functions
    // ------------------------------------------------------------------
    m.def("start_agent",
          [](std::string_view config_file_path,
             std::string_view config_yaml,
             std::string_view env_prefix,
             int32_t app_type,
             std::string_view server_info,
             const std::vector<std::string>& args,
             const std::vector<std::string>& libs,
             NativeLogBridge* log_bridge) {
              pp::AgentOptions options;
              options.config_file_path = config_file_path;
              options.config_yaml = config_yaml;
              options.env_prefix = env_prefix;
              options.app_type = app_type;
              options.server_info = server_info;
              options.args = args;
              options.libs = libs;
              if (log_bridge != nullptr) {
                  // Capture shared C++ state, never the Python wrapper. Logger
                  // callbacks may outlive start_agent's Python frame and, on
                  // failed startup, remain installed until the next start.
                  // deactivate() makes such a retained callback a lock-free
                  // no-op while shared ownership prevents use-after-free.
                  auto queue = log_bridge->queue();
                  options.log_sink = [queue = std::move(queue)](
                                         const char* level,
                                         const char* message) noexcept {
                      queue->enqueue(level, message);
                  };
              }
              // The C++ API reports launch success separately from handle lookup.
              // start_agent() returns the usable handle and raises on a failed
              // launch, which agent.init() degrades to _NullAgent.
              if (!pp::StartAgent(options)) {
                  throw std::runtime_error(
                      "pinpoint agent failed to start; check the native agent log");
              }
              return pp::GlobalAgent();
          },
          py::arg("config_file_path"), py::arg("config_yaml"),
          py::arg("env_prefix"), py::arg("app_type"), py::arg("server_info"),
          py::arg("args"), py::arg("libs"), py::arg("log_bridge") = nullptr,
          py::call_guard<py::gil_scoped_release>());
}
