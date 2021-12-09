#include "dependency_mining_config.hpp"

#include <fstream>

#include "utils/assert.hpp"

namespace opossum {

void assign(uint8_t& value, const nlohmann::json& json_object, const std::string& key) {
  const auto& json_value = json_object.find(key);
  Assert(json_value != json_object.end(), "Did not find '" + key + "' in dependency usage config");
  Assert(json_value->is_number() && *json_value >= 0, "Dependency mining config has invalid value for'" + key + "'");
  value = *json_value;
}

void assign(int64_t& value, const nlohmann::json& json_object, const std::string& key) {
  const auto& json_value = json_object.find(key);
  Assert(json_value != json_object.end(), "Did not find '" + key + "' in dependency usage config");
  Assert(json_value->is_number(), "Dependency mining config has invalid value for'" + key + "'");
  value = *json_value;
}

void assign(std::chrono::high_resolution_clock::duration& value, const nlohmann::json& json_object,
            const std::string& key) {
  const auto& json_value = json_object.find(key);
  Assert(json_value != json_object.end(), "Did not find '" + key + "' in dependency usage config");
  Assert(json_value->is_number(), "Dependency mining config has invalid value for'" + key + "'");
  value = std::chrono::seconds{*json_value};
}

std::shared_ptr<DependencyMiningConfig> process_dependency_mining_config_file(const std::string& file_path) {
  std::ifstream config_file{file_path};
  Assert(config_file.good(), "Mining config file does not exist: " + file_path);
  nlohmann::json config_json;
  config_file >> config_json;
  auto config = static_cast<DependencyMiningConfig>(config_json);
  return std::make_shared<DependencyMiningConfig>(config);
}

void DependencyMiningConfig::output_to_stream(std::ostream& stream) const {
  stream << "DependencyMiningConfig: num_validators " << std::to_string(num_validators)
         << ", max_validation_candidates " << max_validation_candidates << ", max_validation_time "
         << std::chrono::duration_cast<std::chrono::seconds>(max_validation_time).count() << " s" << std::endl;
}

void from_json(const nlohmann::json& json, DependencyMiningConfig& config) {
  assign(config.num_validators, json, "num_validators");
  assign(config.max_validation_candidates, json, "max_validation_candidates");
  assign(config.max_validation_time, json, "max_validation_time");
}

std::ostream& operator<<(std::ostream& stream, const DependencyMiningConfig& dependency_mining_config) {
  dependency_mining_config.output_to_stream(stream);
  return stream;
}

}  // namespace opossum
