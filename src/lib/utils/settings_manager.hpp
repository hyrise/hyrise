#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace opossum {

/**
 * The SettingsManager is the central point for accessing all Setting objects.
 * Settings registered here can be changed from everywhere.
 * Users change settings through the settings meta table
 */
class SettingsManager : public Noncopyable {
 public:
  bool has_setting(const std::string& name) const;
  std::shared_ptr<AbstractSetting> get_setting(const std::string& name) const;
  std::vector<std::string> setting_names() const;

 protected:
  friend class AbstractSetting;
  friend class Hyrise;
  friend class SettingsManagerTest;

  void _add(std::shared_ptr<AbstractSetting> setting);
  void _remove(const std::string& name);

 private:
  std::map<std::string, std::shared_ptr<AbstractSetting>> _settings;
};

}  // namespace opossum
