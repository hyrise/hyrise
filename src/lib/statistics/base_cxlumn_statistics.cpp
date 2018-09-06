#include "base_cxlumn_statistics.hpp"

namespace opossum {

BaseCxlumnStatistics::BaseCxlumnStatistics(const DataType data_type, const float null_value_ratio,
                                           const float distinct_count)
    : _data_type(data_type), _null_value_ratio(null_value_ratio), _distinct_count(distinct_count) {}

DataType BaseCxlumnStatistics::data_type() const { return _data_type; }

float BaseCxlumnStatistics::null_value_ratio() const { return _null_value_ratio; }

float BaseCxlumnStatistics::non_null_value_ratio() const { return 1.0f - _null_value_ratio; }

float BaseCxlumnStatistics::distinct_count() const { return _distinct_count; }

void BaseCxlumnStatistics::set_null_value_ratio(const float null_value_ratio) { _null_value_ratio = null_value_ratio; }

std::shared_ptr<BaseCxlumnStatistics> BaseCxlumnStatistics::without_null_values() const {
  auto clone = this->clone();
  clone->_null_value_ratio = 0.0f;
  return clone;
}

std::shared_ptr<BaseCxlumnStatistics> BaseCxlumnStatistics::only_null_values() const {
  auto clone = this->clone();
  clone->_null_value_ratio = 1.0f;
  return clone;
}

}  // namespace opossum
