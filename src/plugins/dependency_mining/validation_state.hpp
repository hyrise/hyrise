#pragma once

#include <atomic>
#include <chrono>

namespace opossum {

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 */
struct ValidationState {
  enum class State { NotStarted, Running, Over };

  explicit ValidationState(const bool no_limits, const int64_t init_max_validations = 0,
                           const bool use_time_for_validation = false, const Duration init_max_duration = Duration{});
  ValidationState& operator=(const ValidationState& other);

  bool keep_running();
  void set_done();
  bool is_done() const;
  bool time_left() const;

  std::atomic<State> state{State::NotStarted};
  TimePoint validation_begin = TimePoint{};
  Duration validation_duration = Duration{};
  std::atomic_int64_t validation_count = 0;

  bool allow_all;
  int64_t max_validations;
  bool use_time;
  Duration max_duration;
};

}  // namespace opossum
