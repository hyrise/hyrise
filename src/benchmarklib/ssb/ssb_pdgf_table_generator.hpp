#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "abstract_pdgf_table_generator.hpp"
#include "file_based_table_generator.hpp"

namespace hyrise {

// Generates the SSB data by calling PDGF.
class SSBPDGFTableGenerator : virtual public AbstractPDGFTableGenerator {
 public:
  // Constructor for creating a SSBPDGFTableGenerator in a benchmark.
  explicit SSBPDGFTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config,
                                 std::vector<std::string> queries_to_run);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  const std::string _pdgf_schema_config_file() const override;
  void _add_constraints(std::unordered_map<std::string, BenchmarkTableInfo>& table_info_by_name) const final;
};

}  // namespace hyrise
