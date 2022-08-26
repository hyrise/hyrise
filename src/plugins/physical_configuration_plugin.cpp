#include "physical_configuration_plugin.hpp"

#include "hyrise.hpp"
#include "physical_configuration/physical_config.hpp"

namespace hyrise {

std::string PhysicalConfigurationPlugin::description() const {
  return "This is the Hyrise PhysicalConfigurationPlugin";
}

void PhysicalConfigurationPlugin::start() {
  _config_path = std::make_unique<ConfigPath>("PhysicalConfigurationPlugin.ConfigPath");
  _config_path->register_at_settings_manager();
}

void PhysicalConfigurationPlugin::stop() {
  _config_path->unregister_at_settings_manager();
}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
PhysicalConfigurationPlugin::provided_user_executable_functions() {
  return {{"ApplyPhysicalConfiguration", [&]() { this->apply_physical_configuration(); }}};
}

void PhysicalConfigurationPlugin::apply_physical_configuration() {
  // TODO
}

PhysicalConfigurationPlugin::ConfigPath::ConfigPath(const std::string& init_name) : AbstractSetting(init_name) {}

const std::string& PhysicalConfigurationPlugin::ConfigPath::description() const {
  static const auto description = std::string{"Path to JSON file with physical configuration"};
  return description;
}

const std::string& PhysicalConfigurationPlugin::ConfigPath::get() {
  return _value;
}

void PhysicalConfigurationPlugin::ConfigPath::set(const std::string& value) {
  _value = value;
}

EXPORT_PLUGIN(PhysicalConfigurationPlugin)

}  // namespace hyrise
