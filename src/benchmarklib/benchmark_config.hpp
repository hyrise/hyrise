#pragma once

#include <chrono>

#include "encoding_config.hpp"
#include "utils/null_streambuf.hpp"

namespace opossum {

/**
 * IndividualQueries runs each query a number of times and then the next one
 * PermutedQuerySet runs the queries as set permuting their order after each run (this exercises caches)
 */
enum class BenchmarkMode { IndividualQueries, PermutedQuerySet };

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

// View BenchmarkConfig::description to see format of the JSON-version
class BenchmarkConfig {
 public:
  BenchmarkConfig(const BenchmarkMode benchmark_mode, const ChunkOffset chunk_size,
                  const EncodingConfig& encoding_config, const size_t max_num_query_runs, const Duration& max_duration,
                  const Duration& warmup_duration, const UseMvcc use_mvcc,
                  const std::optional<std::string>& output_file_path, const bool enable_scheduler, const uint32_t cores,
                  const uint32_t clients, const bool enable_visualization, const bool verify,
                  const bool cache_binary_tables, const bool enable_jit);

  static BenchmarkConfig get_default_config();

  BenchmarkMode benchmark_mode = BenchmarkMode::IndividualQueries;
  ChunkOffset chunk_size = 100'000;
  EncodingConfig encoding_config = EncodingConfig{};
  size_t max_num_query_runs = 1000;
  Duration max_duration = std::chrono::seconds(60);
  Duration warmup_duration = std::chrono::seconds(0);
  UseMvcc use_mvcc = UseMvcc::No;
  std::optional<std::string> output_file_path = std::nullopt;
  bool enable_scheduler = false;
  uint32_t cores = 0;
  uint32_t clients = 1;
  bool enable_visualization = false;
  bool verify = false;
  bool cache_binary_tables = false;
  bool enable_jit = false;

  static const char* description;

 private:
  BenchmarkConfig() = default;
};

}  // namespace opossum
