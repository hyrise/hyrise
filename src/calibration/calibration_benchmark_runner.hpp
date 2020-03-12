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

    void export_benchmark(const BenchmarkType type, const float SCALE_FACTOR);

  private:
    const std::string& _path_to_dir;
    const OperatorFeatureExport _measurement_export;
    TableFeatureExport _table_export;


    std::shared_ptr<BenchmarkConfig> _config;

    void _run_tcph(const float SCALE_FACTOR) const;
    void _run_tcpds(const float SCALE_FACTOR) const;
    void _run_job(const float SCALE_FACTOR) const;

    void _export_measurements();
};
}  // namespace opossum
