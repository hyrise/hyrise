#include "all_parameter_variant.hpp"

#include "type_cast.hpp"

namespace opossum {
std::string to_string(const AllParameterVariant& x) {
  if (is_parameter_id(x)) {
    return std::string("Placeholder #") + std::to_string(std::get<ParameterID>(x));
  } else if (is_column_id(x)) {
    return std::string("Column #") + std::to_string(std::get<ColumnID>(x));
  } else {
    std::string string;
    std::visit([&](const auto& typed_value) {
    	string = type_cast<std::string>(typed_value);
    }, x);
    return string;
  }
}

AllTypeVariant to_all_type_variant(const AllParameterVariant& x) {
  AllTypeVariant variant;
  std::visit([&](const auto& typed_value) {
    if constexpr (std::is_same_v<std::decay_t<decltype(typed_value)>, ColumnID> || std::is_same_v<std::decay_t<decltype(typed_value)>, ParameterID>) {
      Fail("Passed argument is not an AllTypeVariant");
    } else {
      variant = typed_value;
    }
  }, x);
  return variant;
}

AllParameterVariant to_all_parameter_variant(const AllTypeVariant& x) {
  AllParameterVariant variant;
  std::visit([&](const auto& typed_value) {
    variant = typed_value;
  }, x);
  return variant;
}

}  // namespace opossum

namespace std {
std::ostream& operator<<(std::ostream& out, const opossum::AllParameterVariant& variant) {
  out << to_string(variant);
  return out;
}
}  // namespace std
