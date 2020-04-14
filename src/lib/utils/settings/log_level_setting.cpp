#include "log_level_setting.hpp"

#include "constant_mappings.hpp"
#include "hyrise.hpp"

namespace opossum {

LogLevelSetting::LogLevelSetting(const std::string& init_name) : AbstractSetting(init_name) {}

const std::string& LogLevelSetting::description() const {
  static const auto description = std::string{"Minimal log level of the LogManager"};
  return description;
}

const std::string& LogLevelSetting::get() { return log_level_to_string.left.at(Hyrise::get().log_manager._log_level); }

void LogLevelSetting::set(const std::string& value) {
  Hyrise::get().log_manager._log_level = log_level_to_string.right.at(value);
}

void LogLevelSetting::register_at(SettingsManager& settings_manager) {
  settings_manager._add(std::static_pointer_cast<AbstractSetting>(shared_from_this()));
}

}  // namespace opossum
