#include "benchmark_config.hpp"

namespace opossum {

BenchmarkConfig::BenchmarkConfig(const BenchmarkMode benchmark_mode, const ChunkOffset chunk_size,
                                 const EncodingConfig& encoding_config, const bool indexes, const int64_t max_runs,
                                 const Duration& max_duration, const Duration& warmup_duration,
                                 const std::optional<std::string>& output_file_path, const bool enable_scheduler,
                                 const uint32_t cores, const uint32_t clients, const bool enable_visualization,
                                 const bool verify, const bool cache_binary_tables, const bool sql_metrics)
    : benchmark_mode(benchmark_mode),
      chunk_size(chunk_size),
      encoding_config(encoding_config),
      indexes(indexes),
      max_runs(max_runs),
      max_duration(max_duration),
      warmup_duration(warmup_duration),
      output_file_path(output_file_path),
      enable_scheduler(enable_scheduler),
      cores(cores),
      clients(clients),
      enable_visualization(enable_visualization),
      verify(verify),
      cache_binary_tables(cache_binary_tables),
      sql_metrics(sql_metrics) {}

BenchmarkConfig BenchmarkConfig::get_default_config() { return BenchmarkConfig(); }

// This is intentionally limited to 80 chars per line, as cxxopts does this too and it looks bad otherwise.
const char* BenchmarkConfig::description = R"(
============================
Benchmark Configuration JSON
============================
All options can also be provided as a JSON config file. This must be the only
argument passed in. The options are identical to and behave like the CLI options.
Example:
{
  "scheduler": true,
  "time": 5
}

The JSON config can also include benchmark-specific options (e.g. TPCH's scale
option). They will be parsed like the
CLI options.

{
  "scale": 0.1
}
)";

}  // namespace opossum
