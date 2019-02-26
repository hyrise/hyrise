#pragma once

#include <optional>
#include <string>
#include <vector>
#include "boost/container/small_vector.hpp"

#include "storage/base_segment.hpp"
#include "storage/constraints/row_templated_constraint_checker.hpp"
#include "storage/segment_accessor.hpp"

namespace opossum {

// Each tuple of a unique constraint row is a boost small_vector with three elements already
// preallocated. Why three? We think that most constraints use a maximum of three columns.
using TupleRow = boost::container::small_vector<AllTypeVariant, 3>;

class ConcatenatedConstraintChecker : public RowTemplatedConstraintChecker<TupleRow> {
 public:
  ConcatenatedConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : RowTemplatedConstraintChecker<TupleRow>(table, constraint) {}

  virtual std::shared_ptr<std::vector<TupleRow>> get_inserted_rows(
      std::shared_ptr<const Table> table_to_insert) const {
    auto rows = std::make_shared<std::vector<TupleRow>>();

    std::vector<std::shared_ptr<BaseSegment>> segments;

    for (const auto& chunk : table_to_insert->chunks()) {
      segments.clear();
      for (const auto column_id : this->_constraint.columns) {
        segments.emplace_back(chunk->segments()[column_id]);
      }

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        bool contains_null = false;
        TupleRow row;
        for (const auto& segment : segments) {
          const auto& value = segment->operator[](chunk_offset);
          if (variant_is_null(value)) {
            contains_null = true;
            break;
          }
          row.emplace_back(value);
        }
        if (!contains_null) {
          rows->emplace_back(row);
        }
      }
    }

    return rows;
  }

  virtual bool preprocess_chunk(std::shared_ptr<const Chunk> chunk) {
    const auto& segments = chunk->segments();

    this->_segments.clear();
    for (const auto column_id : this->_constraint.columns) {
      this->_segments.emplace_back(segments[column_id]);
    }

    return true;
  }

  virtual std::optional<TupleRow> get_row(std::shared_ptr<const Chunk> chunk, const ChunkOffset chunk_offset) const {
    auto row = TupleRow();
    row.resize(this->_segments.size());

    for (size_t i = 0; i < row.size(); i++) {
      const auto& segment = this->_segments[i];
      const auto& value = (*segment)[chunk_offset];
      if (variant_is_null(value)) {
        return std::nullopt;
      }
      row[i] = value;
    }
    return row;
  }

 protected:
  std::vector<std::shared_ptr<BaseSegment>> _segments;
};

}  // namespace opossum
