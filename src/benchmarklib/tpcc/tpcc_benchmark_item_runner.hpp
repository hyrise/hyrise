#pragma once

#include <atomic>

#include "abstract_benchmark_item_runner.hpp"

namespace opossum {

class TPCCBenchmarkItemRunner : public AbstractBenchmarkItemRunner {
 public:
  TPCCBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config, int num_warehouses);

  std::string item_name(const BenchmarkItemID item_id) const override;
  const std::vector<BenchmarkItemID>& items() const override;

  const std::vector<int>& weights() const override;

 protected:
  bool _on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) override;

  const int _num_warehouses;
};

}  // namespace opossum
