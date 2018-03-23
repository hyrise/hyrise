#pragma once

#include <cstdint>
#include <string>

#include "json.hpp"

#include "abstract_column_benchmark.hpp"

namespace opossum {

class MultiScenarioColumnBenchmark : public AbstractColumnBenchmark {
 public:
  MultiScenarioColumnBenchmark(nlohmann::json description);

  void run() final;

 private:
  void _output_as_json(nlohmann::json& data);
  void _output_as_csv(const nlohmann::json& data);
  void _generate_statistics(nlohmann::json& benchmark, std::shared_ptr<ValueColumn<int32_t>> value_column);

  nlohmann::json _value_or_default(const nlohmann::json& benchmark, const std::string& key) const;

 private:
  nlohmann::json _description;
  nlohmann::json _context;
};

}  // namespace opossum
