#include "abstract_setting.hpp"

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractSetting::AbstractSetting(const std::string& init_name) : name(init_name) {}

void AbstractSetting::enroll() {
  auto name_copy = name;
  Hyrise::get().settings_manager.add(name_copy, std::static_pointer_cast<AbstractSetting>(shared_from_this()));
}

void AbstractSetting::unenroll() {
  auto name_copy = name;
  Hyrise::get().settings_manager.remove(name_copy);
}

}  // namespace opossum
