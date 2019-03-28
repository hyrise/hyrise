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

class MultiColumnConstraintChecker : public RowTemplatedConstraintChecker<TupleRow> {
 public:
  MultiColumnConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : RowTemplatedConstraintChecker<TupleRow>(table, constraint) {}

  virtual std::vector<TupleRow> get_inserted_rows(std::shared_ptr<const Table> table_to_insert) const {
    std::vector<TupleRow> rows;

    std::vector<std::shared_ptr<BaseSegment>> segments;

    for (const auto& chunk : table_to_insert->chunks()) {
      segments.clear();
      for (const auto& column_id : this->_constraint.columns) {
        segments.emplace_back(chunk->segments()[column_id]);
      }

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        bool contains_null = false;
        TupleRow row;
        for (const auto& segment : segments) {
          const auto& value = (*segment)[chunk_offset];
          if (variant_is_null(value)) {
            contains_null = true;
            break;
          }
          row.emplace_back(value);
        }
        if (!contains_null) {
          rows.emplace_back(row);
        }
      }
    }

    return rows;
  }

  virtual void prepare_read_chunk_cached(std::shared_ptr<const Chunk> chunk) {
    const auto& segments = chunk->segments();

    this->_segments_cached.clear();
    for (const auto& column_id : this->_constraint.columns) {
      this->_segments_cached.emplace_back(segments[column_id]);
    }
  }

  virtual std::optional<TupleRow> get_row_from_cached_chunk(std::shared_ptr<const Chunk> chunk,
                                                            const ChunkOffset chunk_offset) const {
    auto row = TupleRow();
    row.resize(this->_segments_cached.size());

    for (size_t i = 0; i < row.size(); i++) {
      const auto& segment = this->_segments_cached[i];
      const auto& value = (*segment)[chunk_offset];
      if (variant_is_null(value)) {
        return std::nullopt;
      }
      row[i] = value;
    }
    return row;
  }

 protected:
  // These members are the cached state for "prepare_read_chunk_cached" and following methods!
  // Also see documentation of "prepare_read_chunk_cached" in RowTemplatedConstraintChecker
  // for a detailed explanation.

  // Segments of unique columns:
  std::vector<std::shared_ptr<BaseSegment>> _segments_cached;
};

}  // namespace opossum
