
#include "base_segment.hpp"

namespace opossum {

BaseSegment::BaseSegment(const DataType data_type) : _data_type(data_type) {}

DataType BaseSegment::data_type() const { return _data_type; }

}  // namespace opossum
