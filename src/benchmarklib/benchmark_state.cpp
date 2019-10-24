#include "benchmark_state.hpp"

#include <chrono>

namespace opossum {

BenchmarkState::BenchmarkState(const opossum::Duration max_duration) : max_duration(max_duration) {}

// NOLINTNEXTLINE(bugprone-unhandled-self-assignment,cert-oop54-cpp)
BenchmarkState& BenchmarkState::operator=(const BenchmarkState& other) {
  Assert(state != State::Running && other.state != State::Running, "Cannot assign to or from a running benchmark");
  state = other.state.load();
  benchmark_begin = other.benchmark_begin;
  benchmark_duration = other.benchmark_duration;
  max_duration = other.max_duration;

  return *this;
}

bool BenchmarkState::keep_running() {
  switch (state) {
    case State::NotStarted:
      benchmark_begin = std::chrono::high_resolution_clock::now();
      state = State::Running;
      break;
    case State::Over:
      return false;
    default: {}
  }

  benchmark_duration = std::chrono::high_resolution_clock::now() - benchmark_begin;

  // Stop execution if we reached the time limit
  if (benchmark_duration >= max_duration) {
    set_done();
    return false;
  }

  return true;
}

void BenchmarkState::set_done() { state = State::Over; }

bool BenchmarkState::is_done() { return state == State::Over; }

}  // namespace opossum
