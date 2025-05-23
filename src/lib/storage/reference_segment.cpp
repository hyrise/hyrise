#include "reference_segment.hpp"

#include <cstddef>
#include <memory>

#include "all_type_variant.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/pos_lists/abstract_pos_list.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id,
                                   const std::shared_ptr<const AbstractPosList>& pos)
    : AbstractSegment(referenced_table->column_data_type(referenced_column_id)),
      _referenced_table(referenced_table),
      _referenced_column_id(referenced_column_id),
      _pos_list(pos) {
  Assert(_referenced_column_id < _referenced_table->column_count(), "ColumnID out of range.");
  Assert(referenced_table->type() == TableType::Data, "Referenced table must be Data Table.");

  // Theoretically, a ReferenceSegment can become bigger than the input segments of the operator. This can happen, for
  // example, if a hash-based join puts all entries into the same bucket and generates a single segment. It is the duty
  // of the operator to make sure that, once a ReferenceSegment is full, the next one is started. So far, most
  // operators ignore this, simply because we have not experienced the issue and have not considered it to be a
  // priority. This assert makes sure that we become aware of it becoming relevant.
  Assert(pos->size() <= Chunk::MAX_SIZE, "AbstractPosList exceeds Chunk::MAX_SIZE.");
}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  const auto row_id = (*_pos_list)[chunk_offset];

  if (row_id.is_null()) {
    return NULL_VALUE;
  }

  const auto chunk = _referenced_table->get_chunk(row_id.chunk_id);

  return (*chunk->get_segment(_referenced_column_id))[row_id.chunk_offset];
}

const std::shared_ptr<const AbstractPosList>& ReferenceSegment::pos_list() const {
  return _pos_list;
}

const std::shared_ptr<const Table>& ReferenceSegment::referenced_table() const {
  return _referenced_table;
}

ColumnID ReferenceSegment::referenced_column_id() const {
  return _referenced_column_id;
}

ChunkOffset ReferenceSegment::size() const {
  return static_cast<ChunkOffset>(_pos_list->size());
}

std::shared_ptr<AbstractSegment> ReferenceSegment::copy_using_memory_resource(
    MemoryResource& /*memory_resource*/) const {
  // ReferenceSegments are considered as intermediate data structures and are
  // therefore not subject to NUMA-aware chunk migrations.
  Fail("Cannot migrate a ReferenceSegment.");
}

size_t ReferenceSegment::memory_usage(const MemoryUsageCalculationMode mode) const {
  return sizeof(*this) + _pos_list->memory_usage(mode);
}

}  // namespace hyrise
