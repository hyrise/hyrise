#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

namespace opossum {

// ReferenceColumn is a specific column type that stores all its values as position list of a referenced column
class ReferenceColumn : public BaseColumn {
 public:
  // creates a reference column
  // the parameters specify the positions and the referenced column
  ReferenceColumn(const std::shared_ptr<const Table> referenced_table, const ColumnID referenced_column_id,
                  const std::shared_ptr<const PosList> pos);

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  void append(const AllTypeVariant&) override;

  size_t size() const final;

  const std::shared_ptr<const PosList> pos_list() const;
  const std::shared_ptr<const Table> referenced_table() const;

  ColumnID referenced_column_id() const;

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) const override;

  template <typename ContextClass>
  void visit_dereferenced(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> ctx) const {
    /*
    The pos_list might be unsorted. In that case, we would have to jump around from chunk to chunk.
    One-chunk-at-a-time processing should be faster. For this, we place a pair {chunk_offset, original_position}
    into a vector for each chunk. A potential optimization would be to only do this if the pos_list is really
    unsorted.
    */

    std::unordered_map<ChunkID, std::shared_ptr<std::vector<ChunkOffset>>, std::hash<decltype(ChunkID().t)>>
        all_chunk_offsets;

    for (auto row_id : *(_pos_list)) {
      auto iter = all_chunk_offsets.find(row_id.chunk_id);
      if (iter == all_chunk_offsets.end())
        iter = all_chunk_offsets.emplace(row_id.chunk_id, std::make_shared<std::vector<ChunkOffset>>()).first;

      iter->second->emplace_back(row_id.chunk_offset);
    }

    for (auto& pair : all_chunk_offsets) {
      auto& chunk_id = pair.first;
      auto& chunk_offsets = pair.second;

      auto chunk = _referenced_table->get_chunk(chunk_id);
      auto referenced_column = chunk->get_column(_referenced_column_id);

      auto context = std::make_shared<ContextClass>(referenced_column, _referenced_table, ctx, chunk_id, chunk_offsets);
      referenced_column->visit(visitable, context);
    }
  }

  std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;

  size_t estimate_memory_usage() const override;

 protected:
  // After an operator finishes, its shared_ptr reference to the table gets deleted. Thus, the ReferenceColumns need
  // their own shared_ptrs
  const std::shared_ptr<const Table> _referenced_table;

  const ColumnID _referenced_column_id;

  // The position list can be shared amongst multiple columns
  const std::shared_ptr<const PosList> _pos_list;
};

}  // namespace opossum
