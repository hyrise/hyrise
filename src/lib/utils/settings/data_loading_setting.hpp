#pragma once

#include <string>

#include <utils/settings/abstract_setting.hpp>


namespace hyrise {

class DataLoadingSetting : public AbstractSetting {
 public:
  explicit DataLoadingSetting(const std::string& init_name)
    : AbstractSetting{init_name} {}

  const std::string& description() const final {
    static const auto description = std::string{"Data Loading Setting."};
    return description;
  }

  const std::string& get() final {
    return _value;
  }

  void set(const std::string& value) final {
    _value = value;
  }

 private:
  std::string _value;
};

}  // namespace hyrise
