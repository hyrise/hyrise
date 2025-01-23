#pragma once

#include <chrono>
#include <string>
#include <vector>

#include "encoding_config.hpp"
#include "storage/chunk.hpp"

namespace hyrise {

/**
 * "Ordered" runs each item a number of times and then the next one.
 * "Shuffled" runs the items in a random order.
 */
enum class BenchmarkMode { Ordered, Shuffled };

/**
 * What data to generate for a benchmark run. Only used in combination with PDGF.
 */
enum class ColumnsToGenerate { All, OnlyUsedColumns, OnlyUsedTables };

using Duration = std::chrono::nanoseconds;
// `steady_clock` guarantees that the clock is not adjusted while benchmarking.
using TimePoint = std::chrono::steady_clock::time_point;

class BenchmarkConfig {
 public:
  BenchmarkConfig() = default;

  explicit BenchmarkConfig(const ChunkOffset init_chunk_size);

  BenchmarkConfig(const ChunkOffset init_chunk_size, const bool init_cache_binary_tables);

  BenchmarkConfig(const BenchmarkMode init_benchmark_mode, const ChunkOffset init_chunk_size,
                  const EncodingConfig& init_encoding_config,
                  const bool init_dont_generate_table_statistics, const bool init_chunk_indexes,
                  const bool init_table_indexes, const bool init_only_load_data,
                  const bool init_separate_benchmark_cycle_per_query,
                  const int64_t init_max_runs, const Duration& init_max_duration,
                  const Duration& init_warmup_duration, const std::optional<std::string>& init_output_file_path,
                  const bool init_enable_scheduler, const uint32_t init_cores,
                  const uint32_t init_data_preparation_cores, const uint32_t init_clients,
                  const bool init_enable_visualization, const bool init_verify, const bool init_cache_binary_tables,
                  const bool init_enable_pdgf_data_generation, const uint64_t pdgf_project_seed,
                  const uint32_t pdgf_num_cores, const int32_t pdgf_work_unit_size,
                  const ColumnsToGenerate init_columns_to_generate,
                  const bool init_system_metrics, const bool init_pipeline_metrics,
                  const std::vector<std::string>& init_plugins);

  BenchmarkMode benchmark_mode{BenchmarkMode::Ordered};
  ChunkOffset chunk_size{Chunk::DEFAULT_SIZE};
  EncodingConfig encoding_config{};
  bool dont_generate_table_statistics{false};
  bool chunk_indexes{false};
  bool table_indexes{false};
  bool separate_benchmark_cycle_per_query{false};
  bool only_load_data{false};
  int64_t max_runs{-1};
  Duration max_duration{std::chrono::seconds{60}};
  Duration warmup_duration{0};
  std::optional<std::string> output_file_path{};
  bool enable_scheduler{false};
  uint32_t cores{0};
  uint32_t data_preparation_cores{0};
  uint32_t clients{1};
  bool enable_visualization{false};
  bool verify{false};
  bool enable_pdgf_data_generation{false};
  uint32_t pdgf_num_cores{64};
  uint64_t pdgf_project_seed{123456789};
  int32_t pdgf_work_unit_size{128};
  ColumnsToGenerate columns_to_generate{ColumnsToGenerate::All};
  // Defaults to false for internal use. Benchmark, console, and server binaries set caching of benchmark data using
  // binary files to true by default.
  bool cache_binary_tables{false};
  bool system_metrics{false};
  bool pipeline_metrics{false};
  std::vector<std::string> plugins{};
};

}  // namespace hyrise
