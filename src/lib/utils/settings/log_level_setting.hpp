#pragma once

#include "utils/settings/abstract_setting.hpp"
#include "utils/settings_manager.hpp"

namespace opossum {

class LogLevelSetting : public AbstractSetting {
 public:
  explicit LogLevelSetting(const std::string& init_name);

  const std::string& description() const final;

  const std::string& get() final;

  void set(const std::string& value) final;

  void register_at(SettingsManager& settings_manager);
};

}  // namespace opossum
