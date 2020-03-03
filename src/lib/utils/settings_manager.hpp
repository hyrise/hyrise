#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"
#include "utils/settings/abstract_setting.hpp"

namespace opossum {

class SettingsManager : public Noncopyable {
 public:
  bool has_setting(const std::string& name) const;
  std::shared_ptr<AbstractSetting> get_setting(const std::string name) const;
  std::vector<std::string> all_settings() const;

 protected:
  friend class AbstractSetting;
  friend class Hyrise;
  friend class SettingsManagerTest;

  void add(const std::string& name, std::shared_ptr<AbstractSetting> setting);
  void remove(const std::string name);

 private:
  std::map<std::string, std::shared_ptr<AbstractSetting>> _settings;
};

}  // namespace opossum
