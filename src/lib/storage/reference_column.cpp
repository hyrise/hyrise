#include "reference_column.hpp"

#include <memory>
#include <string>
#include <utility>

#include "column_visitable.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

ReferenceColumn::ReferenceColumn(const std::shared_ptr<const Table> referenced_table,
                                 const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _pos_list(pos) {
#if IS_DEBUG
  auto referenced_column = _referenced_table->get_chunk(ChunkID{0})->get_column(referenced_column_id);
  auto reference_col = std::dynamic_pointer_cast<const ReferenceColumn>(referenced_column);

  DebugAssert(!(reference_col), "referenced_column must not be a ReferenceColumn");
#endif
}

const AllTypeVariant ReferenceColumn::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  auto chunk_info = _pos_list->at(chunk_offset);

  if (chunk_info == NULL_ROW_ID) return NULL_VALUE;

  auto chunk = _referenced_table->get_chunk(chunk_info.chunk_id);

  return (*chunk->get_column(_referenced_column_id))[chunk_info.chunk_offset];
}

void ReferenceColumn::append(const AllTypeVariant&) { Fail("ReferenceColumn is immutable"); }

const std::shared_ptr<const PosList> ReferenceColumn::pos_list() const { return _pos_list; }
const std::shared_ptr<const Table> ReferenceColumn::referenced_table() const { return _referenced_table; }
ColumnID ReferenceColumn::referenced_column_id() const { return _referenced_column_id; }

size_t ReferenceColumn::size() const { return _pos_list->size(); }

void ReferenceColumn::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_column(*this, std::move(context));
}

std::shared_ptr<BaseColumn> ReferenceColumn::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // ReferenceColumns are considered as intermediate datastructures and are
  // therefore not subject to NUMA-aware chunk migrations.
  Fail("Cannot migrate a ReferenceColumn");
}

size_t ReferenceColumn::estimate_memory_usage() const {
  return sizeof(*this) + _pos_list->size() * sizeof(decltype(_pos_list)::element_type::value_type);
}

}  // namespace opossum
