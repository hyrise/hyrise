#include <tpch/tpch_benchmark_item_runner.hpp>
#include <benchmark_runner.hpp>
#include <tpch/tpch_table_generator.hpp>
#include <file_based_benchmark_item_runner.hpp>
#include <tpcds/tpcds_table_generator.hpp>
#include <file_based_table_generator.hpp>
#include "calibration_benchmark_runner.hpp"
#include "hyrise.hpp"

namespace opossum {

    CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir):
            _feature_export(OperatorFeatureExport(path_to_dir)),
            _table_export(TableFeatureExport(path_to_dir))
    {
      auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
      config->max_runs = 1;
      config->enable_visualization = false;
      config->chunk_size = 100'000;
      config->cache_binary_tables = true;
      _config = config;
    }

    CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir, std::shared_ptr<BenchmarkConfig> config):
            _feature_export(OperatorFeatureExport(path_to_dir)),
            _table_export(TableFeatureExport(path_to_dir))
    {
        _config = config;
    }

    void CalibrationBenchmarkRunner::run_benchmark(const BenchmarkType type, const float SCALE_FACTOR, const int number_of_executions) {

      std::shared_ptr<BenchmarkRunner> benchmark_runner;
      switch(type){
        case BenchmarkType::TCPH:   benchmark_runner = _build_tcph(SCALE_FACTOR); break;
        case BenchmarkType::TCPDS:  benchmark_runner = _build_tcpds(SCALE_FACTOR); break;
        case BenchmarkType::JOB:    benchmark_runner = _build_job(); break;
        default: throw std::runtime_error("Provided unknown BenchmarkType.");
      }

      for (int i = 0; i < number_of_executions; ++i){
        Hyrise::get().benchmark_runner = benchmark_runner;
        benchmark_runner->run();

        auto& pqp_cache = Hyrise::get().default_pqp_cache;

        for (auto pqp_entry = pqp_cache->unsafe_begin(); pqp_entry != pqp_cache->unsafe_end(); ++pqp_entry) {
          const auto& [query_string, physical_query_plan] = *pqp_entry;
          _feature_export.export_to_csv(physical_query_plan);
        }

        // Clear pqp cache for next benchmark
        pqp_cache->clear();
      }

      const std::vector<std::string> table_names = Hyrise::get().storage_manager.table_names();
      for (const auto &table_name : table_names){
        auto table = Hyrise::get().storage_manager.get_table(table_name);
        _table_export.export_table(std::make_shared<CalibrationTableWrapper>(CalibrationTableWrapper(table, table_name)));

        // Drop table because we do not need it anymore
        Hyrise::get().storage_manager.drop_table(table_name);
      }
    }

    std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tcph(const float SCALE_FACTOR) const {
      auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(_config, false, SCALE_FACTOR);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(
              *_config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, _config), BenchmarkRunner::create_context(*_config));

      return benchmark_runner;
    }

    std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tcpds(const float SCALE_FACTOR) const {
      const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

      auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(_config, query_path, std::unordered_set<std::string>{});
      auto table_generator = std::make_unique<TpcdsTableGenerator>(SCALE_FACTOR, _config);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(*_config, std::move(query_generator), std::move(table_generator),
                                                                opossum::BenchmarkRunner::create_context(*_config));
      return benchmark_runner;
    }

    std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_job() const {
      const auto table_path = "hyrise/imdb_data";
      const auto query_path = "hyrise/third_party/join-order-benchmark";
      const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

      auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(_config, query_path, non_query_file_names);
      auto table_generator = std::make_unique<FileBasedTableGenerator>(_config, table_path);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(*_config, std::move(benchmark_item_runner), std::move(table_generator),
                                                                BenchmarkRunner::create_context(*_config));
      return benchmark_runner;
    }
}