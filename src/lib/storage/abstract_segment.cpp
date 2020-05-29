#include "abstract_segment.hpp"

namespace opossum {

AbstractSegment::AbstractSegment(const DataType data_type) : _data_type(data_type) {}

DataType AbstractSegment::data_type() const { return _data_type; }

}  // namespace opossum
