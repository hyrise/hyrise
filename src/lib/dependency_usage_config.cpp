#include "dependency_usage_config.hpp"

#include <fstream>

#include "utils/assert.hpp"

namespace opossum {

void assign(bool& value, const nlohmann::json& json_object, const std::string& key) {
  if (json_object.find(key) != json_object.end()) {
    value = json_object.at(key);
  } else {
    Fail("Did not find '" + key + "' in dependency usage config");
  }
}

std::shared_ptr<DependencyUsageConfig> process_dependency_config_file(const std::string& file_path) {
  std::ifstream config_file{file_path};
  Assert(config_file.good(), "Config file does not exist: " + file_path);
  nlohmann::json config_json;
  config_file >> config_json;
  auto config = static_cast<DependencyUsageConfig>(config_json);
  return std::make_shared<DependencyUsageConfig>(config);
}

void DependencyUsageConfig::output_to_stream(std::ostream& stream) const {
  const auto str = [](const auto value) { return value ? "on" : "off"; };
  stream << "DependencyUsageConfig: dgr  " << str(enable_groupby_reduction) << ", join2semi "
         << str(enable_join_to_semi) << ", join2pred " << str(enable_join_to_predicate) << ", join_elimination "
         << str(enable_join_elimination) << ", set constraints " << str(allow_preset_constraints) << std::endl;
}

void from_json(const nlohmann::json& json, DependencyUsageConfig& config) {
  assign(config.enable_groupby_reduction, json, "groupby_reduction");
  assign(config.enable_join_to_semi, json, "join_to_semi");
  assign(config.enable_join_to_predicate, json, "join_to_predicate");
  assign(config.enable_join_elimination, json, "join_elimination");
  assign(config.allow_preset_constraints, json, "preset_constraints");
}

std::ostream& operator<<(std::ostream& stream, const DependencyUsageConfig& dependency_usage_config) {
  dependency_usage_config.output_to_stream(stream);
  return stream;
}

}  // namespace opossum
