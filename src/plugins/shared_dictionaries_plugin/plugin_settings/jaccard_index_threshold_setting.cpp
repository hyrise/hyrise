#include "jaccard_index_threshold_setting.hpp"

namespace opossum {

const std::string& JaccardIndexThresholdSetting::description() const {
  static const auto description = std::string{"Threshold for the similarity metric between dictionaries for merging"};
  return description;
}
const std::string& JaccardIndexThresholdSetting::display_name() const { return _display_name; }
const std::string& JaccardIndexThresholdSetting::get() { return _value; }
void JaccardIndexThresholdSetting::set(const std::string& value_string) {
  const auto value = std::stod(value_string);
  if (value < 0.0 || value > 1.0) throw std::out_of_range("Value should be between 0.0 and 1.0 (inclusive).");
  _value = value_string;
}

}  // namespace opossum
