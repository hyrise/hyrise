#pragma once

#include <benchmark_config.hpp>
#include "string"
#include "table_export.hpp"
#include "measurement_export.hpp"

namespace opossum{
enum BenchmarkType{
    TCPH, TCPDS, JOB
};

class BenchmarkBuilder {
  public:
    BenchmarkBuilder(const std::string& path_to_dir);
    BenchmarkBuilder(const std::string& path_to_dir, std::shared_ptr<BenchmarkConfig> config);

    void export_benchmark(const BenchmarkType type, const float SCALE_FACTOR) const;

  private:
    const std::string&      _path_to_dir;
    const MeasurementExport _measurement_export;
    const TableExport       _table_export;


    std::shared_ptr<BenchmarkConfig> _config;

    void _run_tcph(const float SCALE_FACTOR) const;
    void _run_tcpds(const float SCALE_FACTOR) const;
    void _run_job(const float SCALE_FACTOR) const;

    void _export_measurements();
};
}  // namespace opossum
