#pragma once

#include "utils/abstract_plugin.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace hyrise {

class PhysicalConfigurationPlugin : public AbstractPlugin {
 public:
  PhysicalConfigurationPlugin() = default;

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

  void apply_physical_configuration();

 protected:
  class ConfigPath : public AbstractSetting {
   public:
    static inline const std::string DEFAULT_CONFIG_PATH{"./physical_configuration.json"};
    explicit ConfigPath(const std::string& init_name);

    const std::string& description() const final;

    const std::string& get() final;

    void set(const std::string& value) final;

   private:
    std::string _value = DEFAULT_CONFIG_PATH;
  };

  std::unique_ptr<ConfigPath> _config_path;
};

}  // namespace hyrise
