#include "abstract_setting.hpp"

#include "utils/assert.hpp"
#include "utils/settings_manager.hpp"

namespace opossum {

AbstractSetting::AbstractSetting(const std::string& init_name) : name(init_name) {}

void AbstractSetting::enroll() { SettingsManager::get().add(shared_from_this()); }

void AbstractSetting::unenroll() { SettingsManager::get().remove(name); }

}  // namespace opossum
