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

}  // namespace opossum
