#pragma once

#include <chrono>

#include "encoding_config.hpp"
#include "storage/chunk.hpp"

namespace opossum {

/**
 * "Ordered" runs each item a number of times and then the next one
 * "Shuffled" runs the items in a random order
 */
enum class BenchmarkMode { Ordered, Shuffled };

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

class BenchmarkConfig {
 public:
  BenchmarkConfig(const BenchmarkMode benchmark_mode, const ChunkOffset chunk_size,
                  const EncodingConfig& encoding_config, const bool indexes, const int64_t max_runs,
                  const Duration& max_duration, const Duration& warmup_duration,
                  const std::optional<std::string>& output_file_path, const bool enable_scheduler, const uint32_t cores,
                  const uint32_t clients, const bool enable_visualization, const bool verify,
                  const bool cache_binary_tables, const bool sql_metrics);

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
  uint32_t clients = 1;
  bool enable_visualization = false;
  bool verify = false;
  bool cache_binary_tables = false;  // Defaults to false for internal use, but the CLI sets it to true by default
  bool sql_metrics = false;

 private:
  BenchmarkConfig() = default;
};

}  // namespace opossum
