#include "reference_column.hpp"

#include <memory>
#include <string>
#include <utility>

#include "abstract_column_visitor.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

ReferenceColumn::ReferenceColumn(const std::shared_ptr<const Table> referenced_table,
                                 const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
    : BaseColumn(referenced_table->column_data_type(referenced_column_id)),
      _referenced_table(referenced_table),
      _referenced_column_id(referenced_column_id),
      _pos_list(pos) {
  Assert(_referenced_column_id < _referenced_table->column_count(), "ColumnID out of range")
      DebugAssert(referenced_table->type() == TableType::Data, "Referenced table must be Data Table");
}

const AllTypeVariant ReferenceColumn::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  const auto row_id = _pos_list->at(chunk_offset);

  if (row_id.is_null()) return NULL_VALUE;

  auto chunk = _referenced_table->get_chunk(row_id.chunk_id);

  return (*chunk->get_column(_referenced_column_id))[row_id.chunk_offset];
}

void ReferenceColumn::append(const AllTypeVariant&) { Fail("ReferenceColumn is immutable"); }

const std::shared_ptr<const PosList> ReferenceColumn::pos_list() const { return _pos_list; }
const std::shared_ptr<const Table> ReferenceColumn::referenced_table() const { return _referenced_table; }
ColumnID ReferenceColumn::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceColumn::size() const { return _pos_list->size(); }

std::shared_ptr<BaseColumn> ReferenceColumn::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // ReferenceColumns are considered as intermediate datastructures and are
  // therefore not subject to NUMA-aware chunk migrations.
  Fail("Cannot migrate a ReferenceColumn");
}

size_t ReferenceColumn::estimate_memory_usage() const {
  return sizeof(*this) + _pos_list->size() * sizeof(decltype(_pos_list)::element_type::value_type);
}

}  // namespace opossum
