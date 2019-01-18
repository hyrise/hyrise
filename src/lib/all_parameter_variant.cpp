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
	// std::visit([&](const auto& typed_value) {
	// 	string = type_cast<std::string>(typed_value);
	// }, value);
    return string;
  }
}

AllTypeVariant to_all_type_variant(const AllParameterVariant& x) {
  AllTypeVariant variant;
  // TODO
  return variant;
}

AllParameterVariant to_all_parameter_variant(const AllTypeVariant& x) {
  AllParameterVariant variant;
  // TODO
  return variant;
}

}  // namespace opossum

namespace std {
  std::ostream& operator<<(std::ostream& out, const opossum::AllParameterVariant& variant) {
    // out << opossum::type_cast<std::string>(variant);  // TODO
    return out;
  }
}

