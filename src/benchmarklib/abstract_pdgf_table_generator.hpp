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
  explicit AbstractPDGFTableGenerator(float scale_factor, const std::shared_ptr<BenchmarkConfig>& benchmark_config, std::vector<std::pair<BenchmarkItemID, std::string>> queries_to_run);

  std::unordered_map<std::string, BenchmarkTableInfo> generate() override;
  std::unordered_map<std::string, BenchmarkTableInfo> _generate(std::optional<std::string> single_query_string);

 protected:
  virtual std::string _benchmark_name_short() const = 0;
  virtual std::string _pdgf_schema_config_file() const = 0;
  virtual std::string _pdgf_schema_generation_file() const {
   return "default-shm-reflective-generation.xml";
  };
  virtual uint32_t _pdgf_buffer_columns() const {
   return 32u;
  }

  void _collect_columns(const std::string& sql);

  float _scale_factor;
  std::vector<std::pair<BenchmarkItemID, std::string>> _queries_to_run;
  std::shared_ptr<std::set<std::string>> _columns_to_generate;
};
}  // namespace hyrise
