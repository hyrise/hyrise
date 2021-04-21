#include "calibration_benchmark_runner.hpp"

#include <fstream>

#include <magic_enum.hpp>
#include "boost/algorithm/string/case_conv.hpp"

#include "benchmark_runner.hpp"
#include "file_based_benchmark_item_runner.hpp"
#include "file_based_table_generator.hpp"
#include "hyrise.hpp"
#include "jcch/jcch_benchmark_item_runner.hpp"
#include "jcch/jcch_table_generator.hpp"
#include "tpcc/tpcc_benchmark_item_runner.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tpcds/tpcds_table_generator.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/assert.hpp"

namespace opossum {

CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir, bool skew_jcch)
    : _path_to_dir(path_to_dir), _skew_jcch(skew_jcch) {
  _config = std::make_shared<BenchmarkConfig>(BenchmarkConfig::get_default_config());
}

CalibrationBenchmarkRunner::CalibrationBenchmarkRunner(const std::string& path_to_dir,
                                                       std::shared_ptr<BenchmarkConfig> config, bool skew_jcch)
    : _path_to_dir(path_to_dir), _skew_jcch(skew_jcch) {
  _config = config;
}

void CalibrationBenchmarkRunner::run_benchmark(const BenchmarkType type, const float scale_factor,
                                               const int number_of_executions) {
  //auto subdirectory = std::string{magic_enum::enum_name(type)};
  //boost::algorithm::to_lower(subdirectory);
  const auto path = _path_to_dir;  // + "/" + subdirectory;
  std::filesystem::create_directories(path);
  const auto feature_exporter = std::make_shared<OperatorFeatureExporter>(path);
  auto table_exporter = TableFeatureExporter(path);

  const auto benchmark_runner = [this, &type, &scale_factor, &feature_exporter]() {
    switch (type) {
      case BenchmarkType::TPC_H:
        return _build_tpch(scale_factor, feature_exporter);
      case BenchmarkType::TPC_DS:
        return _build_tpcds(scale_factor, feature_exporter);
      case BenchmarkType::TPC_C:
        return _build_tpcc(scale_factor, feature_exporter);
      case BenchmarkType::JOB:
        return _build_job(feature_exporter);
      case BenchmarkType::JCC_H:
        return _build_jcch(scale_factor, feature_exporter);
      default:
        std::cout << "Unhandled case, please address this" << std::endl;
        return std::shared_ptr<BenchmarkRunner>();
    }
  }();

  for (int execution_index = 0; execution_index < number_of_executions; ++execution_index) {
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();
  }

  std::cout << std::endl << "Exporting data for " << magic_enum::enum_name(type) << std::endl;
  feature_exporter->flush();

  const std::vector<std::string> table_names = Hyrise::get().storage_manager.table_names();
  for (const auto& table_name : table_names) {
    auto table = Hyrise::get().storage_manager.get_table(table_name);
    table_exporter.export_table(std::make_shared<CalibrationTableWrapper>(CalibrationTableWrapper(table, table_name)));

    Hyrise::get().storage_manager.drop_table(table_name);
  }
  table_exporter.flush();
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tpch(
    const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const {
  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(_config, false, scale_factor);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(
      *_config, std::move(item_runner), std::make_unique<TPCHTableGenerator>(scale_factor, _config),
      BenchmarkRunner::create_context(*_config), feature_exporter);

  return benchmark_runner;
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tpcds(
    const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const {
  std::string query_path = "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";
  auto filename_blacklist = std::unordered_set<std::string>{};
  const auto blacklist_file_path = "resources/benchmark/tpcds/query_blacklist.cfg";
  std::ifstream blacklist_file(blacklist_file_path);

  if (!blacklist_file) {
    Fail("Cannot open the blacklist file: " + blacklist_file_path);
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
  auto benchmark_runner =
      std::make_shared<BenchmarkRunner>(*_config, std::move(query_generator), std::move(table_generator),
                                        BenchmarkRunner::create_context(*_config), feature_exporter);
  return benchmark_runner;
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tpcc(
    const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const {
  const auto num_warehouses = std::max(static_cast<size_t>(scale_factor), size_t{1});
  _config->cache_binary_tables = false;
  auto item_runner = std::make_unique<TPCCBenchmarkItemRunner>(_config, num_warehouses);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(
      *_config, std::move(item_runner), std::make_unique<TPCCTableGenerator>(num_warehouses, _config),
      BenchmarkRunner::create_context(*_config), feature_exporter);
  return benchmark_runner;
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_job(
    const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const {
  const std::string table_path = "imdb_data";
  const std::string query_path = "third_party/join-order-benchmark";
  const auto non_query_file_names = std::unordered_set<std::string>{"fkindexes.sql", "schema.sql"};

  const auto setup_imdb_command = std::string{"python3 scripts/setup_imdb.py "} + table_path;
  const auto setup_imdb_return_code = system(setup_imdb_command.c_str());
  Assert(setup_imdb_return_code == 0, "setup_imdb.py failed. Did you run the benchmark from the root dir?");

  auto table_generator = std::make_unique<FileBasedTableGenerator>(_config, table_path);
  auto item_runner = std::make_unique<FileBasedBenchmarkItemRunner>(_config, query_path, non_query_file_names);

  auto benchmark_runner =
      std::make_shared<BenchmarkRunner>(*_config, std::move(item_runner), std::move(table_generator),
                                        BenchmarkRunner::create_context(*_config), feature_exporter);
  return benchmark_runner;
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_jcch(
    const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const {
  auto jcch_dbgen_path = std::filesystem::canonical(".") / "third_party/jcch-dbgen";
  Assert(std::filesystem::exists(jcch_dbgen_path / "dbgen"),
         std::string{"JCC-H dbgen not found at "} + jcch_dbgen_path.c_str());
  Assert(std::filesystem::exists(jcch_dbgen_path / "qgen"),
         std::string{"JCC-H qgen not found at "} + jcch_dbgen_path.c_str());

  // Create the jcch_data directory (if needed) and generate the jcch_data/sf-... path
  auto jcch_data_path_str = std::ostringstream{};
  jcch_data_path_str << "../jcch_data/sf-" << std::noshowpoint << scale_factor;
  std::filesystem::create_directories(jcch_data_path_str.str());
  // Success of create_directories is guaranteed by the call to fs::canonical, which fails on invalid paths:
  auto jcch_data_path = std::filesystem::canonical(jcch_data_path_str.str());

  auto table_generator = std::make_unique<JCCHTableGenerator>(jcch_dbgen_path, jcch_data_path, scale_factor, _config);
  auto item_runner = std::make_unique<JCCHBenchmarkItemRunner>(_skew_jcch, jcch_dbgen_path, jcch_data_path, _config,
                                                               false, scale_factor);
  auto benchmark_runner =
      std::make_shared<BenchmarkRunner>(*_config, std::move(item_runner), std::move(table_generator),
                                        BenchmarkRunner::create_context(*_config), feature_exporter);

  return benchmark_runner;
}

}  // namespace opossum
