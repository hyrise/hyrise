#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace opossum {

class SettingsManager : private Noncopyable {
 public:
  bool has_setting(const std::string& name) const;
  void add(const std::shared_ptr<AbstractSetting>& setting);
  const std::shared_ptr<AbstractSetting> get(const std::string& name) const;
  const std::vector<std::shared_ptr<AbstractSetting>> all_settings() const;

 protected:
  friend class Hyrise;
  friend class SettingsManagerTest;

  std::unordered_map<std::string, std::shared_ptr<AbstractSetting>> _settings;
};

}  // namespace opossum
