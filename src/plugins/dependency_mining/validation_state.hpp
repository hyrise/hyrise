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

  explicit ValidationState(const int64_t init_max_validations, const Duration init_max_duration);

  bool keep_running();
  void set_done();
  bool is_done() const;
  bool time_left() const;

  std::atomic<State> state{State::NotStarted};
  TimePoint validation_begin = TimePoint{};
  std::atomic_int64_t validation_count = 0;

  int64_t max_validations;
  Duration max_duration;
  bool use_count;
  bool use_time;
  bool allow_all;
};

}  // namespace opossum
