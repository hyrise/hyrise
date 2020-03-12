#include "mock_setting.hpp"

namespace opossum {

MockSetting::MockSetting(const std::string& init_name)
    : AbstractSetting(init_name), _value(std::string{"mock_value"}), _get_calls(0), _set_calls(0) {}

const std::string& MockSetting::description() const {
  static const auto description = std::string{"mock_description"};
  return description;
}

const std::string& MockSetting::get() {
  _get_calls++;
  return _value;
}

void MockSetting::set(const std::string& value) {
  _set_calls++;
  _value = value;
}

size_t MockSetting::get_calls() const { return _get_calls; }

size_t MockSetting::set_calls() const { return _get_calls; }

}  // namespace opossum
