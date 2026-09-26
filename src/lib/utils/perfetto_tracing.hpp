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

#if HYRISE_WITH_PERFETTO

#include <perfetto.h>

PERFETTO_DEFINE_CATEGORIES(
    perfetto::Category("operator").SetDescription("Execution of physical operators."));

namespace hyrise {

// Starts the tracing session if it is not running yet.
void ensure_tracing_started();

}  // namespace hyrise

// Traces until the end of the current scope {} as one slice, and calls trace_event_end automatically.
#define HYRISE_TRACE_EVENT(category, ...) \
  ::hyrise::ensure_tracing_started();     \
  TRACE_EVENT(category, __VA_ARGS__)

// Explicit begin/end pair for slices that do not map to a scope (e.g., to attach arguments that
// are only known at the end). Every BEGIN must be matched by an END on the same thread.
#define HYRISE_TRACE_EVENT_BEGIN(category, ...) \
  do {                                          \
    ::hyrise::ensure_tracing_started();         \
    TRACE_EVENT_BEGIN(category, __VA_ARGS__);   \
  } while (false)

#define HYRISE_TRACE_EVENT_END(...) TRACE_EVENT_END(__VA_ARGS__)

#else

#define HYRISE_TRACE_EVENT(category, ...) \
  do {                                    \
  } while (false)

#define HYRISE_TRACE_EVENT_BEGIN(category, ...) \
  do {                                          \
  } while (false)

#define HYRISE_TRACE_EVENT_END(...) \
  do {                              \
  } while (false)

#endif
