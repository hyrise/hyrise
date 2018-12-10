#include "benchmark_config.hpp"

namespace opossum {

BenchmarkConfig::BenchmarkConfig(const BenchmarkMode benchmark_mode, const bool verbose, const ChunkOffset chunk_size,
                                 const EncodingConfig& encoding_config, const size_t max_num_query_runs,
                                 const Duration& max_duration, const Duration& warmup_duration, const UseMvcc use_mvcc,
                                 const std::optional<std::string>& output_file_path, const bool enable_scheduler,
                                 const uint32_t cores, const uint32_t clients, const bool enable_visualization,
                                 const bool cache_binary_tables, std::ostream& out)
    : benchmark_mode(benchmark_mode),
      verbose(verbose),
      chunk_size(chunk_size),
      encoding_config(encoding_config),
      max_num_query_runs(max_num_query_runs),
      max_duration(max_duration),
      warmup_duration(warmup_duration),
      use_mvcc(use_mvcc),
      output_file_path(output_file_path),
      enable_scheduler(enable_scheduler),
      cores(cores),
      clients(clients),
      enable_visualization(enable_visualization),
      cache_binary_tables(cache_binary_tables),
      out(out) {}

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
  "verbose": true,
  "scheduler": true,
  "time": 5
}

The JSON config can also include benchmark-specific options (e.g. TPCH's scale
option). They will be parsed like the
CLI options.

{
  "verbose": true,
  "scale": 0.1
}
)";

}  // namespace opossum
