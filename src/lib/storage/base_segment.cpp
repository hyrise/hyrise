
#include "base_segment.hpp"

namespace opossum {

BaseSegment::BaseSegment(const DataType data_type) : _data_type(data_type) {}

DataType BaseSegment::data_type() const { return _data_type; }

void BaseSegment::set_sort_order(opossum::OrderByMode sort_order) { _sort_order.emplace(sort_order); }

const std::optional<OrderByMode> BaseSegment::sort_order() const { return _sort_order; }

}  // namespace opossum
