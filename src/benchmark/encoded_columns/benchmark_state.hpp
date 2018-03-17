#pragma once

#include <cstdint>
#include <chrono>
#include <vector>

#include "clear_cache.hpp"

namespace opossum {

using Clock = std::chrono::high_resolution_clock;
using Duration = Clock::duration;
using TimePoint = Clock::time_point;

class BenchmarkState {
 public:
  enum class State { NotStarted, Running, Over };

 public:
  BenchmarkState(const size_t max_num_iterations, const Duration max_duration);

  bool keep_running();

  template <typename Functor>
  void measure(Functor functor) {
    clear_cache();

    auto begin = Clock::now();
    functor();
    auto end = Clock::now();
    _results.push_back(end - begin);
  }

  std::vector<Duration> results() const { return _results; }
  size_t num_iterations() const { return _num_iterations; }

 private:
  void _init();

 private:
  const size_t _max_num_iterations;
  const Duration _max_duration;

  State _state;
  size_t _num_iterations;
  TimePoint _begin;
  TimePoint _end;

  std::vector<Duration> _results;
};

}  // namespace opossum
