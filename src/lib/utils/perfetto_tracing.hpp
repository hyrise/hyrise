#pragma once

/**
 * Tracing of Hyrise's execution using Perfetto (https://perfetto.dev). Tracing is only compiled in when Hyrise is built
 * with -DENABLE_PERFETTO=ON. Otherwise, all functions below are empty inline functions and Perfetto is neither compiled
 * nor linked.
 *
 * The trace is streamed to `hyrise.perfetto-trace` in the working directory while the process runs
 * and finalized when the process exits. The output path can be changed with the environment variable
 * HYRISE_PERFETTO_TRACE_FILE. Open the resulting file at https://ui.perfetto.dev.
 */

#include <cstdint>
#include <string_view>
#include <type_traits>

#include "magic_enum/magic_enum.hpp"

namespace hyrise {

enum class TraceCategory : uint8_t { Operator, OperatorStep };

#if HYRISE_WITH_PERFETTO

// Starts the tracing session if it is not running yet.
void ensure_tracing_started();

// Explicit begin/end pair for slices (e.g., to attach a description that is only known at the end). Every begin must be
// matched by an end on the same thread.
void trace_event_begin(TraceCategory category, std::string_view name);

// An empty description adds no argument to the slice.
void trace_event_end(TraceCategory category, std::string_view description = {});

#else

inline void trace_event_begin(TraceCategory /*category*/, std::string_view /*name*/) {}

inline void trace_event_end(TraceCategory /*category*/, std::string_view /*description*/ = {}) {}

#endif

// Traces a slice from construction to destruction.
class ScopedTraceEvent final {
 public:
  ScopedTraceEvent(const TraceCategory category, const std::string_view name) : _category{category} {
    trace_event_begin(category, name);
  }

  // Traces an operator step, named after the enum value.
  template <typename Step>
    requires std::is_enum_v<Step>
  explicit ScopedTraceEvent(const Step step)
      : ScopedTraceEvent{TraceCategory::OperatorStep, magic_enum::enum_name(step)} {}

  ScopedTraceEvent(const ScopedTraceEvent&) = delete;
  ScopedTraceEvent(ScopedTraceEvent&&) = delete;
  ScopedTraceEvent& operator=(const ScopedTraceEvent&) = delete;
  ScopedTraceEvent& operator=(ScopedTraceEvent&&) = delete;

  ~ScopedTraceEvent() {
    trace_event_end(_category);
  }

 private:
  TraceCategory _category;
};

}  // namespace hyrise
