#pragma once

#include <memory>

#include "abstract_segment.hpp"
#include "chunk.hpp"
#include "table.hpp"

namespace hyrise {

// PlaceHolder segments do no store any data. They are solely used to recognize accesses to segments that have to be
// created/loaded.
class PlaceHolderSegment : public AbstractSegment {
 public:
  explicit PlaceHolderSegment(const std::shared_ptr<Table>& base_table, const ColumnID column_id, bool nullable = false, ChunkOffset capacity = Chunk::DEFAULT_SIZE);

  // Return the value at a certain position. If you want to write efficient operators, back off!
  // Use values() and null_values() to get the vectors and check the content yourself.
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  // Return the number of entries in the segment.
  ChunkOffset size() const final;

  // Copies a PlaceHolderSegment using a new allocator. This is useful for placing the PlaceHolderSegment on a new NUMA node.
  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const override;

 protected:
  // We don't use ChunkID for now as TPC-H's dbgen does not allow chunk-wise data generation.
  std::shared_ptr<Table> _base_table{};
  ColumnID _column_id{};

  bool _nullable{true};
  ChunkOffset _capacity{Chunk::DEFAULT_SIZE};
};

}  // namespace hyrise
