#include "benchmark_state.hpp"

#include <chrono>
#include <cstdint>

#include "benchmark_config.hpp"
#include "utils/assert.hpp"

namespace hyrise {

BenchmarkState::BenchmarkState(const Duration init_max_duration, const int64_t init_max_runs)
    : max_duration(init_max_duration), max_runs(init_max_runs) {}

BenchmarkState::BenchmarkState(const BenchmarkState& other)
    : state(other.state.load()),
      benchmark_begin(other.benchmark_begin),
      scheduled_runs(other.scheduled_runs),
      max_duration(other.max_duration),
      max_runs(other.max_runs) {
  Assert(state != State::Running, "Cannot assign from a running benchmark.");
}

BenchmarkState& BenchmarkState::operator=(const BenchmarkState& other) {
  if (this == &other) {
    return *this;
  }
  Assert(state != State::Running && other.state != State::Running, "Cannot assign to or from a running benchmark.");
  state = other.state.load();
  benchmark_begin = other.benchmark_begin;
  max_duration = other.max_duration;
  max_runs = other.max_runs;
  scheduled_runs = other.scheduled_runs;

  return *this;
}

// NOLINTNEXTLINE(cppcoreguidelines-noexcept-move-operations,hicpp-noexcept-move,performance-noexcept-move-constructor)
BenchmarkState::BenchmarkState(BenchmarkState&& other)
    : state(other.state.load()),
      benchmark_begin(other.benchmark_begin),
      scheduled_runs(other.scheduled_runs),
      max_duration(other.max_duration),
      max_runs(other.max_runs) {
  Assert(state != State::Running, "Cannot assign from a running benchmark.");
}

// NOLINTNEXTLINE(cppcoreguidelines-noexcept-move-operations,hicpp-noexcept-move,performance-noexcept-move-constructor)
BenchmarkState& BenchmarkState::operator=(BenchmarkState&& other) {
  if (this == &other) {
    return *this;
  }
  Assert(state != State::Running && other.state != State::Running, "Cannot assign to or from a running benchmark.");
  state = other.state.load();
  benchmark_begin = other.benchmark_begin;
  max_duration = other.max_duration;
  max_runs = other.max_runs;
  scheduled_runs = other.scheduled_runs;
  return *this;
}

bool BenchmarkState::keep_running() {
  switch (state) {
    case State::NotStarted:
      benchmark_begin = std::chrono::steady_clock::now();
      state = State::Running;
      break;
    case State::Over:
      return false;
    default: {
    }
  }

  const auto benchmark_duration = std::chrono::steady_clock::now() - benchmark_begin;

  // Stop execution if we reached the time or execution limit.
  if (benchmark_duration >= max_duration || (max_runs >= 0 && scheduled_runs >= max_runs)) {
    state = State::Over;
    return false;
  }

  ++scheduled_runs;
  return true;
}

}  // namespace hyrise
