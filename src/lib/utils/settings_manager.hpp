#pragma once

#include <functional>
#include <unordered_map>

#include "types.hpp"
#include "utils/settings/abstract_setting.hpp"
#include "utils/singleton.hpp"

namespace opossum {

/* Besides the Hyrise class, this should be the only singleton.
 * We need this this to be a singleton since we would lose the control
 * of registered settings on reconstructions otherwise.
 */
class SettingsManager : public Singleton<SettingsManager> {
 public:
  bool has_setting(const std::string& name) const;
  void add(const std::shared_ptr<AbstractSetting>& setting);
  void remove(const std::string& name);
  const std::shared_ptr<AbstractSetting> get_setting(const std::string& name) const;
  const std::vector<std::shared_ptr<AbstractSetting>> all_settings() const;

 private:
  friend class Singleton;
  SettingsManager() {}

  std::map<std::string, std::shared_ptr<AbstractSetting>> _settings;
};

}  // namespace opossum
