#include "benchmark_state.hpp"


namespace opossum {

BenchmarkState::BenchmarkState(const size_t max_num_iterations, const Duration max_duration)
    : _max_num_iterations{max_num_iterations},
      _max_duration{max_duration},
      _state{State::NotStarted} {}

bool BenchmarkState::keep_running() {
  switch (_state) {
    case State::NotStarted:
      _init();
      return true;
    case State::Over:
      return false;
    default: {}
  }

  if (_num_iterations >= _max_num_iterations) {
    _end = Clock::now();
    _state = State::Over;
    return false;
  }

  _end = Clock::now();
  const auto duration = _end - _begin;
  if (duration >= _max_duration) {
    _state = State::Over;
    return false;
  }

  _num_iterations++;

  return true;
}

void BenchmarkState::_init() {
  _state = State::Running;
  _num_iterations = 1u;
  _results = std::vector<Duration>();
  _results.reserve(_max_num_iterations);
  _begin = Clock::now();
}

}  // namespace opossum
