#pragma once

#include <memory>
#include <ostream>

#include "nlohmann/json.hpp"

namespace opossum {

class DependencyUsageConfig {
 public:
  DependencyUsageConfig() = default;

  void output_to_stream(std::ostream& stream) const;

  bool enable_groupby_reduction = false;
  bool enable_join_to_semi = false;
  bool enable_join_to_predicate = false;
  bool enable_join_elimination = false;

  bool allow_preset_constraints = false;
};

std::shared_ptr<DependencyUsageConfig> process_dependency_config_file(const std::string& file_path);

void from_json(const nlohmann::json& json, DependencyUsageConfig& config);

std::ostream& operator<<(std::ostream& stream, const DependencyUsageConfig& dependency_usage_config);

}  // namespace opossum
