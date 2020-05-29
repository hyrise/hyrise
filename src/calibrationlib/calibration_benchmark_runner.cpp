#include <fstream>

#include <benchmark_runner.hpp>
#include <file_based_benchmark_item_runner.hpp>
#include <file_based_table_generator.hpp>
#include <tpch/tpch_benchmark_item_runner.hpp>
#include <tpch/tpch_table_generator.hpp>
#include "calibration_benchmark_runner.hpp"
#include "hyrise.hpp"

namespace opossum {

CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir)
    : _feature_exporter(OperatorFeatureExporter(path_to_dir)), _table_exporter(TableFeatureExporter(path_to_dir)) {
  _config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
}

CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir,
                                                       std::shared_ptr<BenchmarkConfig> config)
    : _feature_exporter(OperatorFeatureExporter(path_to_dir)), _table_exporter(TableFeatureExporter(path_to_dir)) {
  _config = config;
}

void CalibrationBenchmarkRunner::run_benchmark(const BenchmarkType type, const float scale_factor,
                                               const int number_of_executions) {
  std::shared_ptr<BenchmarkRunner> benchmark_runner;
  switch (type) {
    case BenchmarkType::TCPH:
      benchmark_runner = _build_tcph(scale_factor);
      break;
    default:
      throw std::runtime_error("Provided unknown BenchmarkType.");
  }

  for (int execution_index = 0; execution_index < number_of_executions; ++execution_index) {
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();

    // TO DO: this is not what we want. We only get the PQPs of the LAST 1024 queries here
    const auto& pqp_cache = Hyrise::get().default_pqp_cache;
    const auto cache_map = pqp_cache->snapshot();

    for (const auto& [_, entry] : cache_map) {
      _feature_exporter.export_to_csv(entry.value);
    }

    // Clear pqp cache for next benchmark run
    pqp_cache->clear();
  }

  std::cout << std::endl << "- Exporting test data" << std::endl;

  _feature_exporter.flush();

  const std::vector<std::string> table_names = Hyrise::get().storage_manager.table_names();
  for (const auto& table_name : table_names) {
    auto table = Hyrise::get().storage_manager.get_table(table_name);
    _table_exporter.export_table(std::make_shared<CalibrationTableWrapper>(CalibrationTableWrapper(table, table_name)));

    Hyrise::get().storage_manager.drop_table(table_name);
  }

  _table_exporter.flush();
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tcph(const float scale_factor) const {
  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(_config, false, scale_factor);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(*_config, std::move(item_runner),
                                                            std::make_unique<TPCHTableGenerator>(scale_factor, _config),
                                                            BenchmarkRunner::create_context(*_config));

  return benchmark_runner;
}
}  // namespace opossum
