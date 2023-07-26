#include "abstract_segment.hpp"

namespace hyrise {

AbstractSegment::AbstractSegment(const DataType data_type) : _data_type(data_type) {}

DataType AbstractSegment::data_type() const {
  return _data_type;
}

NodeID AbstractSegment::get_numa_node_location() {
  return _numa_node_location;
}

void AbstractSegment::set_numa_node_location(NodeID node_id){
  _numa_node_location = node_id;
}

}  // namespace hyrise
