
#include "base_segment.hpp"

namespace opossum {

BaseSegment::BaseSegment(const DataType data_type) : _data_type(data_type) {}

DataType BaseSegment::data_type() const { return _data_type; }

void BaseSegment::set_sort_order(opossum::OrderByMode sort_order) { _sort_order.emplace(sort_order); }

const std::optional<OrderByMode> BaseSegment::sort_order() const { return _sort_order; }

ChunkOffset BaseSegment::get_non_null_begin(const std::shared_ptr<const PosList>& position_filter) const {
  Fail("Not implemented");
}

ChunkOffset BaseSegment::get_non_null_end(const std::shared_ptr<const PosList>& position_filter) const {
  Fail("Not implemented");
}

ChunkOffset BaseSegment::get_first_bound(const AllTypeVariant& search_value,
                                         const std::shared_ptr<const PosList>& position_filter) const {
  Fail("Not implemented");
}

ChunkOffset BaseSegment::get_last_bound(const AllTypeVariant& search_value,
                                        const std::shared_ptr<const PosList>& position_filter) const {
  Fail("Not implemented");
}

}  // namespace opossum
