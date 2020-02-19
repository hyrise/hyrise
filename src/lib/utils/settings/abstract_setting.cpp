#include "abstract_setting.hpp"

#include "hyrise.hpp"

namespace opossum {

AbstractSetting::AbstractSetting(const std::string& init_name) : name(init_name) {}

void AbstractSetting::enroll() {
	Hyrise::get().settings_manager.add(shared_from_this());
}

}  // namespace opossum
