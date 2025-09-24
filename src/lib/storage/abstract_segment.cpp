#include "abstract_segment.hpp"

#include "all_type_variant.hpp"

namespace hyrise {

AbstractSegment::AbstractSegment(const DataType data_type) : _data_type(data_type) {}

DataType AbstractSegment::data_type() const {
  return _data_type;
}

}  // namespace hyrise
