#pragma once

#include <memory>

#include "abstract_segment.hpp"
#include "chunk.hpp"
#include "hyrise.hpp"
#include "table.hpp"
#include "utils/data_loading_utils.hpp"

namespace hyrise {

// PlaceHolder segments do no store any data. They are solely used to recognize accesses to segments that have to be
// created/loaded.
class PlaceHolderSegment : public AbstractSegment {
 public:
  explicit PlaceHolderSegment(const std::shared_ptr<Table>& init_base_table, const std::string& init_table_name, const ChunkID init_chunk_id,
                              const ColumnID init_column_id, bool init_nullable, ChunkOffset init_capacity = Chunk::DEFAULT_SIZE);

  // Return the value at a certain position. If you want to write efficient operators, back off!
  // Use values() and null_values() to get the vectors and check the content yourself.
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  // Return the number of entries in the segment.
  ChunkOffset size() const final;

  // Copies a PlaceHolderSegment using a new allocator. This is useful for placing the PlaceHolderSegment on a new NUMA node.
  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const override;

  std::shared_ptr<AbstractSegment> load_and_return_segment() const;

  std::shared_ptr<Table> base_table{};
  std::string table_name{};
  ChunkID chunk_id{INVALID_CHUNK_ID};
  ColumnID column_id{INVALID_COLUMN_ID};
  bool nullable{true};
  ChunkOffset capacity{Chunk::DEFAULT_SIZE};
};

}  // namespace hyrise
