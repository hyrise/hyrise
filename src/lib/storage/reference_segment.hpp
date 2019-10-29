#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "storage/pos_list.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

// ReferenceSegment is a specific segment type that stores all its values as position list of a referenced segment
class ReferenceSegment : public BaseSegment {
 public:
  // creates a reference segment
  // the parameters specify the positions and the referenced column
  ReferenceSegment(const std::shared_ptr<const Table>& referenced_table, const ColumnID referenced_column_id,
                   const std::shared_ptr<const PosList>& pos);

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  ChunkOffset size() const final;

  const std::shared_ptr<const PosList>& pos_list() const;
  const std::shared_ptr<const Table>& referenced_table() const;

  ColumnID referenced_column_id() const;

  std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;

  size_t estimate_memory_usage() const override;

 protected:
  // After an operator finishes, its shared_ptr reference to the table gets deleted. Thus, the ReferenceSegments need
  // their own shared_ptrs
  const std::shared_ptr<const Table> _referenced_table;

  const ColumnID _referenced_column_id;

  // The position list can be shared amongst multiple segments
  const std::shared_ptr<const PosList> _pos_list;
};

}  // namespace opossum
