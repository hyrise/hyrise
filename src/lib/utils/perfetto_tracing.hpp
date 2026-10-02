#pragma once

/**
 * Tracing of Hyrise's execution using Perfetto (https://perfetto.dev). Tracing is only compiled in when Hyrise is built
 * with -DENABLE_PERFETTO=ON. Otherwise, all macros below expand to nothing (their arguments are not evaluated) and
 * Perfetto is neither compiled nor linked.
 *
 * The trace is streamed to `hyrise.perfetto-trace` in the working directory while the process runs
 * and finalized when the process exits. The output path can be changed with the environment variable
 * HYRISE_PERFETTO_TRACE_FILE. Open the resulting file at https://ui.perfetto.dev.
 */

#include <string_view>

namespace hyrise {

#if HYRISE_WITH_PERFETTO

// Starts the tracing session if it is not running yet.
void ensure_tracing_started();

// Explicit begin/end pair for slices (e.g., to attach a description that is only known at the end). Every begin must be
// matched by an end on the same thread.
void trace_event_begin(std::string_view category, std::string_view name);

// An empty description adds no argument to the slice.
void trace_event_end(std::string_view category, std::string_view description = {});

#else

inline void trace_event_begin(std::string_view /*category*/, std::string_view /*name*/) {}

inline void trace_event_end(std::string_view /*category*/, std::string_view /*description*/ = {}) {}

#endif

}  // namespace hyrise
