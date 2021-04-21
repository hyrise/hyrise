#pragma once

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "operator_feature_exporter.hpp"
#include "table_feature_exporter.hpp"

namespace opossum {
enum BenchmarkType { TPC_H, TPC_C, TPC_DS, JOB, JCC_H, Calibration };

class CalibrationBenchmarkRunner {
 public:
  explicit CalibrationBenchmarkRunner(const std::string& path_to_dir, bool skew_jcch = false);
  CalibrationBenchmarkRunner(const std::string& path_to_dir, std::shared_ptr<BenchmarkConfig> config,
                             bool skew_jcch = false);

  void run_benchmark(const BenchmarkType type, const float scale_factor, const int number_of_executions);

 private:
  std::shared_ptr<BenchmarkRunner> _build_tpch(const float scale_factor,
                                               const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const;
  std::shared_ptr<BenchmarkRunner> _build_tpcds(const float scale_factor,
                                                const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const;
  std::shared_ptr<BenchmarkRunner> _build_tpcc(const float scale_factor,
                                               const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const;
  std::shared_ptr<BenchmarkRunner> _build_job(const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const;
  std::shared_ptr<BenchmarkRunner> _build_jcch(const float scale_factor,
                                               const std::shared_ptr<OperatorFeatureExporter>& feature_exporter) const;

  std::shared_ptr<BenchmarkConfig> _config;
  const std::string _path_to_dir;
  const bool _skew_jcch;
};
}  // namespace opossum
