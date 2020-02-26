#include <tpch/tpch_benchmark_item_runner.hpp>
#include <benchmark_runner.hpp>
#include <tpch/tpch_table_generator.hpp>
#include <file_based_benchmark_item_runner.hpp>
#include <tpcds/tpcds_table_generator.hpp>
#include <file_based_table_generator.hpp>
#include "benchmark_builder.hpp"
#include "hyrise.hpp"

namespace opossum {

    BenchmarkBuilder::BenchmarkBuilder(const std::string& path_to_dir):
        _path_to_dir(path_to_dir),
        _measurement_export(MeasurementExport(path_to_dir)),
        _table_export(TableExport(path_to_dir))
    {
      auto config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
      config->max_runs = 1;
      config->enable_visualization = false;
      config->chunk_size = 100'000;
      config->cache_binary_tables = true;
      _config = config;
    }

    BenchmarkBuilder::BenchmarkBuilder(const std::string& path_to_dir, std::shared_ptr<BenchmarkConfig> config):
        _path_to_dir(path_to_dir),
        _measurement_export(MeasurementExport(path_to_dir)),
        _table_export(TableExport(path_to_dir))
    {
        _config = config;
    }

    void BenchmarkBuilder::export_benchmark(const BenchmarkType type, const float SCALE_FACTOR) const {
      switch(type){
        case BenchmarkType::TCPH:   _run_tcph(SCALE_FACTOR); break;
        case BenchmarkType::TCPDS:  _run_tcpds(SCALE_FACTOR); break;
        case BenchmarkType::JOB:    _run_job(SCALE_FACTOR); break;
      }

      auto& pqp_cache = Hyrise::get().default_pqp_cache;

      for (auto pqp_entry = pqp_cache->unsafe_begin(); pqp_entry != pqp_cache->unsafe_end(); ++pqp_entry) {
        const auto& [query_string, physical_query_plan] = *pqp_entry;
        _measurement_export.export_to_csv(physical_query_plan);
      }

      // Clear pqp cache for next benchmark
      pqp_cache->clear();

      const std::vector<std::string> table_names = Hyrise::get().storage_manager.table_names();
      for (const auto &table_name : table_names){
        auto table = Hyrise::get().storage_manager.get_table(table_name);
        _table_export.export_table(std::make_shared<CalibrationTableWrapper>(CalibrationTableWrapper(table, table_name)));

        // Drop table because we do not need it anymore
        Hyrise::get().storage_manager.drop_table(table_name);
      }
    }

    void BenchmarkBuilder::_run_tcph(const float SCALE_FACTOR) const {
      constexpr auto USE_PREPARED_STATEMENTS = false;

      // const std::vector<BenchmarkItemID> tpch_query_ids_benchmark = {BenchmarkItemID{5}};
      // auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(config, USE_PREPARED_STATEMENTS, SCALE_FACTOR, tpch_query_ids_benchmark);
      auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(_config, USE_PREPARED_STATEMENTS, SCALE_FACTOR);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(
              *_config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(SCALE_FACTOR, _config), BenchmarkRunner::create_context(*_config));
      Hyrise::get().benchmark_runner = benchmark_runner;
      benchmark_runner->run();
    }

    void BenchmarkBuilder::_run_tcpds(const float SCALE_FACTOR) const {
      const std::string query_path = "hyrise/resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

      auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(_config, query_path, std::unordered_set<std::string>{});
      auto table_generator = std::make_unique<TpcdsTableGenerator>(SCALE_FACTOR, _config);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(*_config, std::move(query_generator), std::move(table_generator),
                                                                opossum::BenchmarkRunner::create_context(*_config));
      Hyrise::get().benchmark_runner = benchmark_runner;
      benchmark_runner->run();
    }

    void BenchmarkBuilder::_run_job(const float SCALE_FACTOR) const { //TODO Remove SCALE_FACTOR HERE
      const auto table_path = "hyrise/imdb_data";
      const auto query_path = "hyrise/third_party/join-order-benchmark";
      const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

      auto benchmark_item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(_config, query_path, non_query_file_names);
      auto table_generator = std::make_unique<FileBasedTableGenerator>(_config, table_path);
      auto benchmark_runner = std::make_shared<BenchmarkRunner>(*_config, std::move(benchmark_item_runner), std::move(table_generator),
                                                                BenchmarkRunner::create_context(*_config));

      Hyrise::get().benchmark_runner = benchmark_runner;
      benchmark_runner->run();
    }
}