#pragma once

#include <atomic>

#include "benchmark_config.hpp"

namespace opossum {

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 */
struct BenchmarkState {
  enum class State { NotStarted, Running, Over };

  explicit BenchmarkState(const Duration max_duration);
  BenchmarkState& operator=(const BenchmarkState& other);

  bool keep_running();
  void set_done();
  bool is_done();

  std::atomic<State> state{State::NotStarted};
  TimePoint benchmark_begin = TimePoint{};
  Duration benchmark_duration = Duration{};

  Duration max_duration;
};

}  // namespace opossum
