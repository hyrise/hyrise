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

// Operator: the execution of a physical operator. OperatorStep: a step within an operator (e.g., Probing).
// OperatorJob: a unit of work within a step that might run in parallel with others (e.g., probing one partition).
enum class TraceCategory : uint8_t { Operator, OperatorStep, OperatorJob };

namespace detail {

// Use ScopedTraceEvent instead of these functions, so that slices are also ended when an exception is thrown. Every
// begin must be matched by an end on the same thread.
#if HYRISE_WITH_PERFETTO

void trace_event_begin(TraceCategory category, std::string_view name);

// An empty description adds no argument to the slice.
void trace_event_end(TraceCategory category, std::string_view description = {});

#else

inline void trace_event_begin(TraceCategory /*category*/, std::string_view /*name*/) {}

inline void trace_event_end(TraceCategory /*category*/, std::string_view /*description*/ = {}) {}

#endif

}  // namespace detail

// Traces a slice from construction until end() is called or, at the latest, until destruction (e.g., when an
// exception is thrown or the scope is left early).
class ScopedTraceEvent final {
 public:
  ScopedTraceEvent(const TraceCategory category, const std::string_view name) : _category{category} {
    detail::trace_event_begin(category, name);
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
    end();
  }

  // Ends the slice before the end of the scope. The description (e.g., only known once the work is done) is attached to
  // the slice. Subsequent calls (including the one from the destructor) have no effect.
  void end(const std::string_view description = {}) {
    if (_ended) {
      return;
    }
    _ended = true;
    detail::trace_event_end(_category, description);
  }

 private:
  TraceCategory _category;
  bool _ended{false};
};

}  // namespace hyrise
