#include "benchmark_config.hpp"

namespace opossum {

BenchmarkConfig::BenchmarkConfig(const BenchmarkMode init_benchmark_mode, const ChunkOffset init_chunk_size,
                                 const EncodingConfig& init_encoding_config, const bool init_indexes,
                                 const int64_t init_max_runs, const Duration& init_max_duration,
                                 const Duration& init_warmup_duration,
                                 const std::optional<std::string>& init_output_file_path,
                                 const bool init_enable_scheduler, const uint32_t init_cores,
                                 const uint32_t init_clients, const bool init_enable_visualization,
                                 const bool init_verify, const bool init_cache_binary_tables,
                                 const bool init_sql_metrics)
    : benchmark_mode(init_benchmark_mode),
      chunk_size(init_chunk_size),
      encoding_config(init_encoding_config),
      indexes(init_indexes),
      max_runs(init_max_runs),
      max_duration(init_max_duration),
      warmup_duration(init_warmup_duration),
      output_file_path(init_output_file_path),
      enable_scheduler(init_enable_scheduler),
      cores(init_cores),
      clients(init_clients),
      enable_visualization(init_enable_visualization),
      verify(init_verify),
      cache_binary_tables(init_cache_binary_tables),
      sql_metrics(init_sql_metrics) {}

BenchmarkConfig BenchmarkConfig::get_default_config() { return BenchmarkConfig(); }

}  // namespace opossum
