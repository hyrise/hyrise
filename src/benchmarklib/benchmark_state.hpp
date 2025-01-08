#pragma once

#include <atomic>

#include "benchmark_config.hpp"

namespace hyrise {

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 */
struct BenchmarkState {
  enum class State { NotStarted, Running, Over };

  explicit BenchmarkState(const Duration init_max_duration, const int64_t init_max_runs);
  BenchmarkState& operator=(const BenchmarkState& other);

  bool keep_running();

  std::atomic<State> state{State::NotStarted};
  TimePoint benchmark_begin = TimePoint{};
  int64_t scheduled_runs{0};

  Duration max_duration;
  int64_t max_runs;
};

}  // namespace hyrise
