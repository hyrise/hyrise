#include "abstract_setting.hpp"

#include "hyrise.hpp"

namespace opossum {

void AbstractSetting::enroll() const {
	const auto pointer = shared_from_this();
	Hyrise::get().settings_manager.add(pointer);
}

}  // namespace opossum
