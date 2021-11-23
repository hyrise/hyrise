#pragma once

#include <memory>
#include <ostream>

#include "nlohmann/json.hpp"

namespace opossum {

class DependencyMiningConfig {
 public:
  DependencyMiningConfig() = default;

  void output_to_stream(std::ostream& stream) const;

  // When set to 0, dependencies will not be mined. Values < 0 are not permitted.
  uint8_t num_validators = 1;
  // When set to a negative value, do not restrict candidates.
  // When set to 0, dependencies will not be mined.
  // If used in combination with max_validation_time, the plugin stops after whatever is reached first.
  int64_t max_validation_candidates = -1;
  // When set to a negative value, do not restrict validation time.
  // When set to 0, dependencies will not be mined.
  // If used in combination with max_validation_candidates, the plugin stops after whatever is reached first.
  std::chrono::high_resolution_clock::duration max_validation_time = std::chrono::seconds{-1};
};

std::shared_ptr<DependencyMiningConfig> process_dependency_mining_config_file(const std::string& file_path);

void from_json(const nlohmann::json& json, DependencyMiningConfig& config);

std::ostream& operator<<(std::ostream& stream, const DependencyMiningConfig& dependency_mining_config);

}  // namespace opossum
