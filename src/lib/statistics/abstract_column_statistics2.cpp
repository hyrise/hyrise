#include "abstract_column_statistics2.hpp"

namespace opossum {

AbstractColumnStatistics2::AbstractColumnStatistics2(const DataType data_type, const float null_value_ratio, const float distinct_count):
  _data_type(data_type), _null_value_ratio(null_value_ratio), _distinct_count(distinct_count) {

}

DataType AbstractColumnStatistics2::data_type() const {
  return _data_type;
}

float AbstractColumnStatistics2::null_value_ratio() const {
  return _null_value_ratio;
}

float AbstractColumnStatistics2::non_null_value_ratio() const {
  return 1.0f - _null_value_ratio;
}

float AbstractColumnStatistics2::distinct_count() const {
  return _distinct_count;
}

void AbstractColumnStatistics2::set_null_value_ratio(const float null_value_ratio) {
  _null_value_ratio = null_value_ratio;
}

std::shared_ptr<AbstractColumnStatistics2> AbstractColumnStatistics2::without_null_values() const {
  auto clone = this->clone();
  clone->_null_value_ratio = 0.0f;
  return clone;
}

}  // namespace opossum
