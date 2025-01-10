#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "abstract_pdgf_table_generator.hpp"
#include "abstract_table_generator.hpp"
#include "benchmark_config.hpp"

namespace hyrise {

// Generates the SSB data by calling PDGF.
class SSBPDGFTableGenerator : virtual public AbstractPDGFTableGenerator {
 public:
  // Constructor for creating a SSBPDGFTableGenerator in a benchmark.
  explicit SSBPDGFTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config,
                                 std::vector<std::string> queries_to_run);

 protected:
  const std::string _pdgf_schema_config_file() const override;
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const final;
};

}  // namespace hyrise
