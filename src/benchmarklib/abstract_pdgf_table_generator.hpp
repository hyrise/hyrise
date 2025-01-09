#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <set>

#include "abstract_table_generator.hpp"
#include "storage/chunk.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"
#include "types.hpp"

namespace hyrise {

class Chunk;
class Table;

/**
 * Generating table by invoking the external PDGF data generator and pulling the results into Hyrise via shared memory.
 */
class AbstractPDGFTableGenerator : public AbstractTableGenerator {
 public:
  // Constructor for creating a AbstractPDGFTableGenerator in a benchmark
  explicit AbstractPDGFTableGenerator(float scale_factor, uint32_t pdgf_work_unit_size,
                                      bool only_generate_used_columns, bool partial_data_generate_whole_tables,
                                      const std::shared_ptr<BenchmarkConfig>& benchmark_config, std::vector<std::string> queries_to_run);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;

 protected:
  void _collect_columns(const std::string& sql, bool whole_table);

  float _scale_factor;
  uint32_t _pdgf_work_unit_size;
  uint32_t _num_cores;
  bool _only_generate_partial_data;
  bool _partial_data_generate_whole_tables;
  std::vector<std::string> _queries_to_run;
  std::shared_ptr<std::set<std::string>> _columns_to_generate;
};
}  // namespace hyrise
