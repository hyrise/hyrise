#include "base_column_statistics.hpp"

namespace opossum {

void BaseColumnStatistics::set_null_value_ratio(const float null_value_ratio) {
  _non_null_value_ratio = 1.f - null_value_ratio;
}

DataType BaseColumnStatistics::data_type() const { return _data_type; }

float BaseColumnStatistics::null_value_ratio() const { return 1.f - _non_null_value_ratio; }

std::ostream& operator<<(std::ostream& os, BaseColumnStatistics& obj) { return obj._print_to_stream(os); }

}  // namespace opossum
