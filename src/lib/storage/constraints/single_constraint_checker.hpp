#pragma once

#include <optional>
#include <algorithm>
#include <string>
#include <vector>

#include "operators/validate.hpp"
#include "storage/constraints/row_templated_constraint_checker.hpp"
#include "storage/segment_accessor.hpp"

namespace opossum {

template <typename T>
class SingleConstraintChecker : public RowTemplatedConstraintChecker<T> {
 public:
  SingleConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : RowTemplatedConstraintChecker<T>(table, constraint) {
    Assert(constraint.columns.size() == 1, "Only one column constraints allowed for SingleConstraintChecker");
  }

  virtual std::shared_ptr<std::vector<T>> get_inserted_rows(std::shared_ptr<const Table> table_to_insert) const {
    auto values = std::make_shared<std::vector<T>>();

    for (const auto& chunk : table_to_insert->chunks()) {
      const auto segment = chunk->segments()[this->_constraint.columns[0]];
      const auto segment_accessor = create_segment_accessor<T>(segment);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        std::optional<T> value = segment_accessor->access(chunk_offset);
        if (value.has_value()) {
          values->emplace_back(value.value());
        }
      }
    }
    return values;
  }

  virtual bool preprocess_chunk(std::shared_ptr<const Chunk> chunk) {
    const auto segment = chunk->segments()[this->_constraint.columns[0]];
    this->segment_accessor = create_segment_accessor<T>(segment);

    // If values are to be inserted and this is a dictionary segment, check if any of the inserted
    // values are contained in the dictionary segment. If at least one of them is contained,
    // we have to check the segment completely (to respect MVCC data), otherwise the
    // normal unique check for this segment can be skipped.
    auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (!dictionary_segment || !this->_values_to_insert) {
      return true;
    }

    const auto dictionary = dictionary_segment->dictionary();
    bool need_check = false;
    for (const auto value : *this->_values_to_insert) {
      if (std::binary_search(dictionary->begin(), dictionary->end(), value)) {
        need_check = true;
        break;
      }
    }
    return need_check;
  }

  virtual std::optional<T> get_row(std::shared_ptr<const Chunk> chunk, const ChunkOffset chunk_offset) const {
    return this->segment_accessor->access(chunk_offset);
  }

 protected:
  std::shared_ptr<BaseSegmentAccessor<T>> segment_accessor;
};

}  // namespace opossum
