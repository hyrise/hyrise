#pragma once

#include <cxxopts.hpp>

#include <chrono>
#include <iostream>
#include <unordered_map>

#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

/**
 * IndividualQueries runs each query a number of times and then the next one
 * PermutedQuerySets runs the queries as sets permuting their order after each run (this exercises caches)
 */
enum class BenchmarkMode { IndividualQueries, PermutedQuerySets };

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

using NamedQuery = std::pair<std::string, std::string>;
using NamedQueries = std::vector<NamedQuery>;

/**
 * @return std::cout if `verbose` is true, otherwise returns a discarding stream
 */
std::ostream& get_out_stream(const bool verbose);

struct QueryBenchmarkResult {
  size_t num_iterations = 0;
  Duration duration = Duration{};
};

using QueryID = size_t;
using BenchmarkResults = std::unordered_map<std::string, QueryBenchmarkResult>;

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 */
struct BenchmarkState {
  enum class State { NotStarted, Running, Over };

  BenchmarkState(const size_t max_num_iterations, const Duration max_duration);

  bool keep_running();

  State state{State::NotStarted};
  TimePoint begin = TimePoint{};
  TimePoint end = TimePoint{};

  size_t num_iterations = 0;
  size_t max_num_iterations;
  Duration max_duration;
};

struct BenchmarkConfig {
  BenchmarkConfig(const BenchmarkMode benchmark_mode, const bool verbose, const ChunkOffset chunk_size,
                  const EncodingType encoding_type, const size_t max_num_query_runs, const Duration& max_duration,
                  const UseMvcc use_mvcc, const std::optional<std::string>& output_file_path,
                  const bool enable_scheduler, const bool enable_visualization, std::ostream& out);

  const BenchmarkMode benchmark_mode;
  const bool verbose;
  const ChunkOffset chunk_size;
  const EncodingType encoding_type;
  const size_t max_num_query_runs;
  const Duration max_duration;
  const UseMvcc use_mvcc;
  const std::optional<std::string> output_file_path;
  const bool enable_scheduler;
  const bool enable_visualization;
  std::ostream& out;
};

}  // namespace opossum
