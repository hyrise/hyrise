#pragma once

#include <atomic>

#include "tpch/tpch_benchmark_item_runner.hpp"

namespace opossum {

// TODO Doc
class JCCHBenchmarkItemRunner : public TPCHBenchmarkItemRunner {
 public:
  // Constructor for a JCCHBenchmarkItemRunner containing all TPC-H queries
  JCCHBenchmarkItemRunner(const std::string& jcch_path, const std::shared_ptr<BenchmarkConfig>& config, bool use_prepared_statements,
                          float scale_factor);

  // Constructor for a JCCHBenchmarkItemRunner containing a subset of TPC-H queries
  JCCHBenchmarkItemRunner(const std::string& jcch_path, const std::shared_ptr<BenchmarkConfig>& config, bool use_prepared_statements,
                          float scale_factor, const std::vector<BenchmarkItemID>& items);

  std::string item_name(const BenchmarkItemID item_id) const override;

 protected:
  bool _on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) override;
  
  void _load_params();

  const std::string _jcch_path;
  std::array<std::vector<std::vector<std::string>>, 22> _all_params;
};

}  // namespace opossum
