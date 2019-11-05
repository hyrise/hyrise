
#include "base_segment.hpp"

namespace opossum {

BaseSegment::BaseSegment(const DataType data_type) : _data_type(data_type) {}

DataType BaseSegment::data_type() const { return _data_type; }

SegmentAccessStatistics_T& BaseSegment::access_statistics() const {
  return _access_statistics;
}

}  // namespace opossum
