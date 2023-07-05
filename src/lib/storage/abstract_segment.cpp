#include "abstract_segment.hpp"

namespace hyrise {

AbstractSegment::AbstractSegment(const DataType data_type) : _data_type(data_type) {}

DataType AbstractSegment::data_type() const {
  return _data_type;
}

NodeID AbstractSegment::numa_node_location() {
  return INVALID_NODE_ID;
}

}  // namespace hyrise
