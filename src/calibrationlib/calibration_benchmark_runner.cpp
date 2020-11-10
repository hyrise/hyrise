#include <fstream>

#include "boost/algorithm/string/case_conv.hpp"
#include <magic_enum.hpp>

#include <benchmark_runner.hpp>
#include <file_based_benchmark_item_runner.hpp>
#include <file_based_table_generator.hpp>
#include <tpch/tpch_benchmark_item_runner.hpp>
#include <tpch/tpch_table_generator.hpp>
#include "calibration_benchmark_runner.hpp"
#include "hyrise.hpp"
#include "utils/assert.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "tpcds/tpcds_table_generator.hpp"

namespace opossum {

CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir) : _path_to_dir(path_to_dir) {
  _config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
  _config->cache_binary_tables = true;
}

CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir,
                                                       std::shared_ptr<BenchmarkConfig> config)
    : _path_to_dir(path_to_dir) {
  _config = config;
}

void CalibrationBenchmarkRunner::run_benchmark(const BenchmarkType type, const float scale_factor,
                                               const int number_of_executions, const int item_runs) {
  _config->max_runs = item_runs;
  auto subdirectory = std::string{magic_enum::enum_name(type)};
  boost::algorithm::to_lower(subdirectory);
  const auto path = _path_to_dir + "/" + subdirectory;
  const auto feature_exporter = std::make_shared<OperatorFeatureExporter>(path);
  auto table_exporter = TableFeatureExporter(path);

  std::shared_ptr<BenchmarkRunner> benchmark_runner;
    switch (type) {
    case BenchmarkType::TPC_H:
      benchmark_runner = _build_tpch(scale_factor, feature_exporter);
      break;
    case BenchmarkType::TPC_DS:
      benchmark_runner = _build_tpcds(scale_factor, feature_exporter);
      break;
    default:
      Fail("Provided unknown BenchmarkType.");
  }

  for (int execution_index = 0; execution_index < number_of_executions; ++execution_index) {
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }

  std::cout << std::endl << "- Exporting test data for " << magic_enum::enum_name(type) << std::endl;

  feature_exporter->flush();

  const std::vector<std::string> table_names = Hyrise::get().storage_manager.table_names();
  for (const auto& table_name : table_names) {
    auto table = Hyrise::get().storage_manager.get_table(table_name);
    table_exporter.export_table(std::make_shared<CalibrationTableWrapper>(CalibrationTableWrapper(table, table_name)));

    Hyrise::get().storage_manager.drop_table(table_name);
  }

  table_exporter.flush();
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tpch(const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const {
  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(_config, false, scale_factor);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(
      *_config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, _config),
      BenchmarkRunner::create_context(*_config), feature_exporter);

  return benchmark_runner;
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tpcds(const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const {
  std::string query_path = "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    std::cerr << "Cannot open the blacklist file: " << blacklist_file_path << "\n";
  } else {
    std::string filename;
    while (std::getline(blacklist_file, filename)) {
      if (filename.size() > 0 && filename.at(0) != '#') {
        filename_blacklist.emplace(filename);
      }
    }
    blacklist_file.close();
  }

  auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(_config, query_path, filename_blacklist);
  auto table_generator = std::make_unique<TPCDSTableGenerator>(scale_factor, _config);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(*_config, std::move(query_generator), std::move(table_generator),
                                          BenchmarkRunner::create_context(*_config), feature_exporter);
  return benchmark_runner;
}

}  // namespace opossum
