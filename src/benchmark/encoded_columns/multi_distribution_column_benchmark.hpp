#pragma once

#include <cstdint>
#include <string>

#include "json.hpp"

#include "storage/base_column.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/value_column.hpp"

#include "abstract_column_benchmark.hpp"
#include "generate_calibration_description.hpp"

namespace opossum {

class MultiDistributionColumnBenchmark : public AbstractColumnBenchmark {
 public:
  MultiDistributionColumnBenchmark(CalibrationType calibration_type, nlohmann::json description);

  void run() final;

 private:
  void _output_as_json(nlohmann::json& data);
  void _output_as_csv(const nlohmann::json& data);
  void _generate_statistics(nlohmann::json& benchmark, std::shared_ptr<ValueColumn<int32_t>> value_column);

  nlohmann::json _value_or_default(const nlohmann::json& benchmark, const std::string& key) const;

 private:
  const CalibrationType _calibration_type;
  nlohmann::json _description;
  nlohmann::json _context;
};

}  // namespace opossum
