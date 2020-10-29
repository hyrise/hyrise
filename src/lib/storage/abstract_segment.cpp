#include "abstract_segment.hpp"

namespace opossum {

AbstractSegment::AbstractSegment(const DataType data_type) : _data_type(data_type) {}

DataType AbstractSegment::data_type() const { return _data_type; }

bool AbstractSegment::contains_no_null_values() const { return _contains_no_null_values; };

void AbstractSegment::set_contains_no_null_values(const bool contains_no_null_values) {
	_contains_no_null_values = contains_no_null_values;
};

}  // namespace opossum
