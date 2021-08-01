#include "shared_dictionaries_plugin_settings.hpp"

namespace opossum {

const std::string& JaccardIndexThresholdSetting::description() const {
  static const auto description = std::string{"Threshold for the similarity metric between dictionaries for merging"};
  return description;
}
const std::string& JaccardIndexThresholdSetting::display_name() const { return _display_name; }
const std::string& JaccardIndexThresholdSetting::get() { return _value; }
void JaccardIndexThresholdSetting::set(const std::string& value) { _value = value; }

}  // namespace opossum
