#pragma once

#include <atomic>
#include <cstdint>

#include "benchmark_config.hpp"

namespace hyrise {

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 */
struct BenchmarkState {
  enum class State : uint8_t { NotStarted, Running, Over };

  ~BenchmarkState() = default;
  explicit BenchmarkState(const Duration init_max_duration, const int64_t init_max_runs);
  BenchmarkState(const BenchmarkState& other);
  BenchmarkState& operator=(const BenchmarkState& other);
  // NOLINTNEXTLINE(cppcoreguidelines-noexcept-move-operations,hicpp-noexcept-move,performance-noexcept-move-constructor)
  BenchmarkState(BenchmarkState&& other);
  // NOLINTNEXTLINE(cppcoreguidelines-noexcept-move-operations,hicpp-noexcept-move,performance-noexcept-move-constructor)
  BenchmarkState& operator=(BenchmarkState&& other);

  bool keep_running();

  std::atomic<State> state{State::NotStarted};
  TimePoint benchmark_begin;
  // No unsigned int type because `max_runs = -1` is the default for unlimited runs, and it its easier to compare that
  // way (no cast required).
  int64_t scheduled_runs{0};

  Duration max_duration;
  int64_t max_runs;
};

}  // namespace hyrise
