#include "abstract_column_statistics.hpp"

namespace opossum {

AbstractColumnStatistics::AbstractColumnStatistics(const DataType data_type, const float null_value_ratio,
                                                   const float distinct_count)
    : _data_type(data_type), _null_value_ratio(null_value_ratio), _distinct_count(distinct_count) {}

DataType AbstractColumnStatistics::data_type() const { return _data_type; }

float AbstractColumnStatistics::null_value_ratio() const { return _null_value_ratio; }

float AbstractColumnStatistics::non_null_value_ratio() const { return 1.0f - _null_value_ratio; }

float AbstractColumnStatistics::distinct_count() const { return _distinct_count; }

void AbstractColumnStatistics::set_null_value_ratio(const float null_value_ratio) {
  _null_value_ratio = null_value_ratio;
}

std::shared_ptr<AbstractColumnStatistics> AbstractColumnStatistics::without_null_values() const {
  auto clone = this->clone();
  clone->_null_value_ratio = 0.0f;
  return clone;
}

}  // namespace opossum
