#pragma once

#include <iostream>
#include <chrono>
#include <unordered_map>
#include <cxxopts.hpp>

#include "storage/encoding_type.hpp"
#include "storage/chunk.hpp"

namespace opossum {

cxxopts::Options cli_options_description{"TPCH Benchmark", ""};

auto x = cli_options_description.

/**
 * IndividualQueries runs each query a number of times and then the next one
 * PermutedQuerySets runs the queries as sets permuting their order after each run (this exercises caches)
 */
enum class BenchmarkMode { IndividualQueries, PermutedQuerySets };

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

using NamedQuery = std::pair<std::string, std::string>;
using NamedQueries = std::vector<NamedQuery>;

// Used to config out()
auto verbose_benchmark = false;

/**
 * @return std::cout if `verbose_benchmark` is true, otherwise returns a discarding stream
 */
std::ostream& out();

struct QueryBenchmarkResult {
  size_t num_iterations = 0;
  Duration duration = Duration{};
};

using QueryID = size_t;
using BenchmarkResults = std::unordered_map<std::string, QueryBenchmarkResult>;

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 *
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
  BenchmarkConfig(const BenchmarkMode benchmark_mode, const ChunkOffset chunk_size, const EncodingType encoding_type,
                  const size_t max_num_query_runs, const Duration& max_duration, const UseMvcc use_mvcc,
                  const std::optional<std::string>& output_file_path, const bool enable_visualization);

  const BenchmarkMode benchmark_mode = BenchmarkMode::IndividualQueries;
  const ChunkOffset chunk_size = Chunk::MAX_SIZE;
  const EncodingType encoding_type = EncodingType::Dictionary;
  const size_t max_num_query_runs = 1000;
  const Duration max_duration = std::chrono::seconds{5};
  const UseMvcc use_mvcc = UseMvcc::No;
  const std::optional<std::string> output_file_path = std::nullopt;
  const bool enable_visualization = false;
};


}  // namespace opossum