#pragma once

#include <algorithm>
#include <optional>
#include <string>

#include "operators/validate.hpp"
#include "storage/constraints/base_constraint_checker.hpp"
#include "storage/segment_accessor.hpp"

namespace opossum {

template <typename T>
class SingleConstraintChecker : public BaseBaseConstraintChecker<T> {
 public:
  SingleConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : BaseBaseConstraintChecker<T>(table, constraint) {
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

  virtual bool prepare_read_chunk(std::shared_ptr<const Chunk> chunk) {
    const auto segment = chunk->segments()[this->_constraint.columns[0]];
    this->segment_accessor = create_segment_accessor<T>(segment);

    auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(segment);
    if (!dictionary_segment || !this->_values_to_insert) {
      return true;
    }

    // if no inserted value found in dictionary:
    // --> skip checking the whole segment
    const auto dictionary = dictionary_segment->dictionary();
    bool found = false;
    for (const auto value : *this->_values_to_insert) {
      if (std::binary_search(dictionary->begin(), dictionary->end(), value)) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }

    return true;
  }

  virtual std::optional<T> get_row(std::shared_ptr<const Chunk> chunk, const ChunkOffset chunk_offset) const {
    return this->segment_accessor->access(chunk_offset);
  }

 protected:
  std::shared_ptr<BaseSegmentAccessor<T>> segment_accessor;
};

}  // namespace opossum
