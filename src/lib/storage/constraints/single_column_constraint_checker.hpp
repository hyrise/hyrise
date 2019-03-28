#pragma once

#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "operators/validate.hpp"
#include "storage/constraints/row_templated_constraint_checker.hpp"
#include "storage/segment_accessor.hpp"

namespace opossum {

template <typename T>
class SingleColumnConstraintChecker : public RowTemplatedConstraintChecker<T> {
 public:
  SingleColumnConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : RowTemplatedConstraintChecker<T>(table, constraint) {
    Assert(constraint.columns.size() == 1,
           "Constraint spans multiple columns, which is not allowed for SingleColumnConstraintChecker");
  }

  virtual std::vector<T> get_inserted_rows(std::shared_ptr<const Table> table_to_insert) const {
    std::vector<T> values;

    for (const auto& chunk : table_to_insert->chunks()) {
      const auto segment = chunk->segments()[this->_constraint.columns[0]];
      const auto segment_accessor = create_segment_accessor<T>(segment);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        std::optional<T> value = segment_accessor->access(chunk_offset);
        if (value.has_value()) {
          values.emplace_back(value.value());
        }
      }
    }
    return values;
  }

  virtual void prepare_read_chunk_cached(std::shared_ptr<const Chunk> chunk) {
    this->_segment_cached = chunk->segments()[this->_constraint.columns[0]];
    this->_segment_accessor_cached = create_segment_accessor<T>(this->_segment_cached);
  }

  virtual bool is_cached_chunk_check_required(std::shared_ptr<const Chunk> chunk) const {
    // If values are to be inserted (indicated by empty values vector) and this is a
    // dictionary segment, check if any of the inserted values are contained in the
    // dictionary segment. If at least one of them is contained, we have to check
    // the segment completely (to respect MVCC data), otherwise the normal unique check
    // for this segment can be skipped.
    auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(this->_segment_cached);
    if (!dictionary_segment || this->_values_to_insert.size() != 0) {
      return true;
    }

    const auto dictionary = dictionary_segment->dictionary();
    bool need_check = false;
    for (const auto& value : this->_values_to_insert) {
      if (std::binary_search(dictionary->begin(), dictionary->end(), value)) {
        need_check = true;
        break;
      }
    }
    return need_check;
  }

  virtual std::optional<T> get_row_from_cached_chunk(std::shared_ptr<const Chunk> chunk,
                                                     const ChunkOffset chunk_offset) const {
    return this->_segment_accessor_cached->access(chunk_offset);
  }

 protected:
  // These members are the cached state for "prepare_read_chunk_cached" and following methods!
  // Also see documentation of "prepare_read_chunk_cached" in RowTemplatedConstraintChecker
  // for a detailed explanation.

  // Segment and belonging segment accessor of current chunk to be checked:
  // (Segment is required as well for special handling of dictionary segment
  //  in "is_cached_chunk_check_required)
  std::shared_ptr<BaseSegment> _segment_cached;
  std::shared_ptr<BaseSegmentAccessor<T>> _segment_accessor_cached;
};

}  // namespace opossum
