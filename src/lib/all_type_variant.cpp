#include "all_type_variant.hpp"

#include <iostream>

#include "type_cast.hpp"

namespace opossum {

bool is_floating_point_data_type(const DataType data_type) {
  return data_type == DataType::Float || data_type == DataType::Double;
}

}  // namespace opossum

namespace std {
	std::ostream& operator<<(std::ostream& out, const opossum::AllTypeVariant& variant) {
		out << opossum::type_cast<std::string>(variant);
		return out;
	}
}
