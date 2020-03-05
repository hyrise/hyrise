#include "abstract_setting.hpp"

#include "hyrise.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractSetting::AbstractSetting(const std::string& init_name) : name(init_name) {}

void AbstractSetting::enroll() {
  Hyrise::get().settings_manager.add(std::static_pointer_cast<AbstractSetting>(shared_from_this()));
}

void AbstractSetting::unenroll() { Hyrise::get().settings_manager.remove(name); }

//std::string AbstractSetting::name() const {return _name;}

}  // namespace opossum
