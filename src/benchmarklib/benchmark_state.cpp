#include "benchmark_state.hpp"

#include <chrono>

#include "benchmark_config.hpp"
#include "utils/assert.hpp"

namespace hyrise {

BenchmarkState::BenchmarkState(const Duration init_max_duration, const int64_t init_max_runs)
    : max_duration(init_max_duration), max_runs(init_max_runs) {}

// NOLINTNEXTLINE(bugprone-unhandled-self-assignment,cert-oop54-cpp)
BenchmarkState& BenchmarkState::operator=(const BenchmarkState& other) {
  Assert(state != State::Running && other.state != State::Running, "Cannot assign to or from a running benchmark.");
  state = other.state.load();
  benchmark_begin = other.benchmark_begin;
  max_duration = other.max_duration;
  max_runs = other.max_runs;
  runs = other.runs;

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

  // Stop execution if we reached the time limit
  if (benchmark_duration >= max_duration || (max_runs >= 0 && ++runs > max_runs)) {
    state = State::Over;
    return false;
  }

  return true;
}

}  // namespace hyrise
