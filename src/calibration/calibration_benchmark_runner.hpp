#pragma once

#include <benchmark_config.hpp>
#include "string"
#include "table_feature_export.hpp"
#include "operator_feature_export.hpp"

namespace opossum{
enum BenchmarkType{
    TCPH, TCPDS, JOB
};

class CalibrationBenchmarkRunner {
  public:
    CalibrationBenchmarkRunner(const std::string& path_to_dir);
    CalibrationBenchmarkRunner(const std::string& path_to_dir, std::shared_ptr<BenchmarkConfig> config);

    void run_benchmark(const BenchmarkType type, const float SCALE_FACTOR, const int number_of_executions);

  private:
    const OperatorFeatureExport _feature_export;
    TableFeatureExport _table_export;


    std::shared_ptr<BenchmarkConfig> _config;

    std::shared_ptr<BenchmarkRunner> _build_tcph(const float SCALE_FACTOR) const;
    std::shared_ptr<BenchmarkRunner> _build_tcpds(const float SCALE_FACTOR) const;
    std::shared_ptr<BenchmarkRunner> _build_job() const;
};
}  // namespace opossum
