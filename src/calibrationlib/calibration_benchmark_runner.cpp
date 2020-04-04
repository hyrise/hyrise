#include <fstream>

#include <benchmark_runner.hpp>
#include <file_based_benchmark_item_runner.hpp>
#include <file_based_table_generator.hpp>
#include <tpcds/tpcds_table_generator.hpp>
#include <tpch/tpch_benchmark_item_runner.hpp>
#include <tpch/tpch_table_generator.hpp>
#include "calibration_benchmark_runner.hpp"
#include "hyrise.hpp"

namespace {
const std::unordered_set<std::string> filename_blacklist() {
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
  return filename_blacklist;
}
}  // namespace

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
    case BenchmarkType::TCPDS:
      benchmark_runner = _build_tcpds(scale_factor);
      break;
    default:
      throw std::runtime_error("Provided unknown BenchmarkType.");
  }

  for (int execution_index = 0; execution_index < number_of_executions; ++execution_index) {
    Hyrise::get().benchmark_runner = benchmark_runner;
    benchmark_runner->run();

    auto& pqp_cache = Hyrise::get().default_pqp_cache;

    for (auto pqp_entry = pqp_cache->unsafe_begin(); pqp_entry != pqp_cache->unsafe_end(); ++pqp_entry) {
      const auto& [query_string, physical_query_plan] = *pqp_entry;
      _feature_exporter.export_to_csv(physical_query_plan);
    }

    // Clear pqp cache for next benchmark run
    pqp_cache->clear();
  }

  const std::vector<std::string> table_names = Hyrise::get().storage_manager.table_names();
  for (const auto& table_name : table_names) {
    auto table = Hyrise::get().storage_manager.get_table(table_name);
    _table_exporter.export_table(std::make_shared<CalibrationTableWrapper>(CalibrationTableWrapper(table, table_name)));

    Hyrise::get().storage_manager.drop_table(table_name);
  }
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tcph(const float scale_factor) const {
  auto item_runner = std::make_unique<TPCHBenchmarkItemRunner>(_config, false, scale_factor);
  auto benchmark_runner = std::make_shared<BenchmarkRunner>(*_config, std::move(item_runner),
                                                            std::make_unique<TPCHTableGenerator>(scale_factor, _config),
                                                            BenchmarkRunner::create_context(*_config));

  return benchmark_runner;
}

std::shared_ptr<BenchmarkRunner> CalibrationBenchmarkRunner::_build_tcpds(const float scale_factor) const {
  const auto valid_scale_factors = std::array{1, 1000, 3000, 10000, 30000, 100000};

  const auto& find_result = std::find(valid_scale_factors.begin(), valid_scale_factors.end(), scale_factor);
  Assert(find_result != valid_scale_factors.end(),
         "TPC-DS benchmark only supports scale factor 1 (qualification only), 1000, 3000, 10000, 30000 and 100000.");

  const std::string query_path = "resources/benchmark/tpcds/tpcds-result-reproduction/query_qualification";

  auto query_generator = std::make_unique<FileBasedBenchmarkItemRunner>(_config, query_path, filename_blacklist());
  auto table_generator = std::make_unique<TpcdsTableGenerator>(scale_factor, _config);
  auto benchmark_runner =
      std::make_shared<BenchmarkRunner>(*_config, std::move(query_generator), std::move(table_generator),
                                        opossum::BenchmarkRunner::create_context(*_config));
  return benchmark_runner;
}
}  // namespace opossum
