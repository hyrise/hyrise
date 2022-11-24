#pragma once

#include <chrono>

#include "encoding_config.hpp"
#include "storage/chunk.hpp"

namespace hyrise {

/**
 * "Ordered" runs each item a number of times and then the next one
 * "Shuffled" runs the items in a random order
 */
enum class BenchmarkMode { Ordered, Shuffled };

using Duration = std::chrono::nanoseconds;
// steady_clock guarantees that the clock is not adjusted while benchmarking
using TimePoint = std::chrono::steady_clock::time_point;

class BenchmarkConfig {
 public:
  BenchmarkConfig(const BenchmarkMode init_benchmark_mode, const ChunkOffset init_chunk_size,
                  const EncodingConfig& init_encoding_config, const bool init_indexes, const int64_t init_max_runs,
                  const Duration& init_max_duration, const Duration& init_warmup_duration,
                  const std::optional<std::string>& init_output_file_path, const bool init_enable_scheduler,
                  const uint32_t init_cores, const uint32_t init_data_preparation_cores, const uint32_t init_clients,
                  const bool init_enable_visualization, const bool init_verify, const bool init_cache_binary_tables,
                  const bool init_metrics);

  static BenchmarkConfig get_default_config();

  BenchmarkMode benchmark_mode = BenchmarkMode::Ordered;
  ChunkOffset chunk_size = Chunk::DEFAULT_SIZE;
  EncodingConfig encoding_config = EncodingConfig{};
  bool indexes = false;
  int64_t max_runs = -1;
  Duration max_duration = std::chrono::seconds(60);
  Duration warmup_duration = std::chrono::seconds(0);
  std::optional<std::string> output_file_path = std::nullopt;
  bool enable_scheduler = false;
  uint32_t cores = 0;
  uint32_t data_preparation_cores = 0;
  uint32_t clients = 1;
  bool enable_visualization = false;
  bool verify = false;
  bool cache_binary_tables = false;  // Defaults to false for internal use, but the CLI sets it to true by default
  bool metrics = false;

 private:
  BenchmarkConfig() = default;
};

}  // namespace hyrise
