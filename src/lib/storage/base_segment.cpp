
#include "base_segment.hpp"

namespace opossum {

uint32_t BaseSegment::_id_counter = 0u;

BaseSegment::BaseSegment(const DataType data_type) : _data_type(data_type), _id{_id_counter++} {}

DataType BaseSegment::data_type() const { return _data_type; }

uint32_t BaseSegment::id() const { return _id; }

}  // namespace opossum
