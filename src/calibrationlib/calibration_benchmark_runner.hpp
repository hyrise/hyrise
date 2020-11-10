#pragma once

#include "string"

#include <benchmark_config.hpp>
#include <benchmark_runner.hpp>

#include "operator_feature_exporter.hpp"
#include "table_feature_exporter.hpp"

namespace opossum {
enum BenchmarkType { TPC_H, TPC_C, TPC_DS };

class CalibrationBenchmarkRunner {
 public:
  explicit CalibrationBenchmarkRunner(const std::string& path_to_dir);
  CalibrationBenchmarkRunner(const std::string& path_to_dir, std::shared_ptr<BenchmarkConfig> config);

  void run_benchmark(const BenchmarkType type, const float scale_factor, const int number_of_executions,
                     const int item_runs);

 private:
  std::shared_ptr<BenchmarkRunner> _build_tpch(const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const;
  std::shared_ptr<BenchmarkRunner> _build_tpcds(const float scale_factor, const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const;

  std::shared_ptr<BenchmarkConfig> _config;
  std::string _path_to_dir;
};
}  // namespace opossum
