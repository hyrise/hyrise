#pragma once

#include "utils/settings/abstract_setting.hpp"

namespace opossum {

/**
 * This is a mock of a setting.
 * It can be used for tests.
 */
class MockSetting : public AbstractSetting {
 public:
  explicit MockSetting(const std::string& init_name);

  const std::string& description() const final;

  const std::string& get() final;

  const std::string& get_display_name() final;

  void set(const std::string& value) final;

  size_t get_calls() const;

  size_t set_calls() const;

 private:
  std::string _value;
  std::string _display_name;
  size_t _get_calls;
  size_t _set_calls;
};

}  // namespace opossum
