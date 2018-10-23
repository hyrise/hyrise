#include "reference_segment.hpp"

#include <memory>
#include <string>
#include <utility>

#include "abstract_segment_visitor.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList>& pos)
    : BaseSegment(referenced_table->column_data_type(referenced_column_id)),
      _referenced_table(referenced_table),
      _referenced_column_id(referenced_column_id),
      _pos_list(pos) {
  Assert(_referenced_column_id < _referenced_table->column_count(), "ColumnID out of range")
      DebugAssert(referenced_table->type() == TableType::Data, "Referenced table must be Data Table");
}

const AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  const auto row_id = _pos_list->at(chunk_offset);

  if (row_id.is_null()) return NULL_VALUE;

  auto chunk = _referenced_table->get_chunk(row_id.chunk_id);

  return (*chunk->get_segment(_referenced_column_id))[row_id.chunk_offset];
}

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const { return _pos_list; }
const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const { return _referenced_table; }
ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceSegment::size() const { return _pos_list->size(); }

std::shared_ptr<BaseSegment> ReferenceSegment::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // ReferenceSegments are considered as intermediate datastructures and are
  // therefore not subject to NUMA-aware chunk migrations.
  Fail("Cannot migrate a ReferenceSegment");
}

size_t ReferenceSegment::estimate_memory_usage() const {
  return sizeof(*this) + _pos_list->size() * sizeof(decltype(_pos_list)::element_type::value_type);
}

}  // namespace opossum
