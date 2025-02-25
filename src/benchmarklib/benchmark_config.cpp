#include "benchmark_config.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "encoding_config.hpp"
#include "types.hpp"

namespace hyrise {

BenchmarkConfig::BenchmarkConfig(const ChunkOffset init_chunk_size) : chunk_size{init_chunk_size} {}

BenchmarkConfig::BenchmarkConfig(const ChunkOffset init_chunk_size, const bool init_cache_binary_tables)
    : chunk_size{init_chunk_size}, cache_binary_tables{init_cache_binary_tables} {}

BenchmarkConfig::BenchmarkConfig(const bool init_enable_hyrise_liveliness_timer,
                                 const BenchmarkMode init_benchmark_mode, const ChunkOffset init_chunk_size,
                                 const EncodingConfig& init_encoding_config,
                                 const bool init_dont_generate_table_statistics, const bool init_chunk_indexes,
                                 const bool init_table_indexes, const bool init_only_load_data,
                                 const bool init_separate_benchmark_cycle_per_query,
                                 const int64_t init_max_runs,
                                 const Duration& init_max_duration, const Duration& init_warmup_duration,
                                 const std::optional<std::string>& init_output_file_path,
                                 const bool init_enable_scheduler, const uint32_t init_cores,
                                 const uint32_t init_data_preparation_cores, const uint32_t init_clients,
                                 const bool init_enable_visualization, const bool init_verify,
                                 const bool init_cache_binary_tables, const std::string init_binary_tables_cache_directory,
                                 const bool init_enable_pdgf_data_generation, const uint64_t init_pdgf_project_seed,
                                 const uint32_t init_pdgf_num_cores, const int32_t init_pdgf_work_unit_size,
                                 const bool init_pdgf_disable_micro_benchmarks, const ColumnsToGenerate init_columns_to_generate,
                                 const bool init_system_metrics,
                                 const bool init_pipeline_metrics, const std::vector<std::string>& init_plugins)
    : enable_hyrise_liveliness_timer{init_enable_hyrise_liveliness_timer},
      benchmark_mode{init_benchmark_mode},
      chunk_size{init_chunk_size},
      encoding_config{init_encoding_config},
      dont_generate_table_statistics{init_dont_generate_table_statistics},
      chunk_indexes{init_chunk_indexes},
      table_indexes{init_table_indexes},
      only_load_data{init_only_load_data},
      separate_benchmark_cycle_per_query{init_separate_benchmark_cycle_per_query},
      max_runs{init_max_runs},
      max_duration{init_max_duration},
      warmup_duration{init_warmup_duration},
      output_file_path{init_output_file_path},
      enable_scheduler{init_enable_scheduler},
      cores{init_cores},
      data_preparation_cores{init_data_preparation_cores},
      clients{init_clients},
      enable_visualization{init_enable_visualization},
      verify{init_verify},
      enable_pdgf_data_generation{init_enable_pdgf_data_generation},
      pdgf_num_cores{init_pdgf_num_cores},
      pdgf_project_seed{init_pdgf_project_seed},
      pdgf_work_unit_size{init_pdgf_work_unit_size},
      pdgf_disable_micro_benchmarks{init_pdgf_disable_micro_benchmarks},
      columns_to_generate{init_columns_to_generate},
      cache_binary_tables{init_cache_binary_tables},
      binary_tables_cache_directory{init_binary_tables_cache_directory},
      system_metrics{init_system_metrics},
      pipeline_metrics{init_pipeline_metrics},
      plugins{init_plugins} {}

}  // namespace hyrise
