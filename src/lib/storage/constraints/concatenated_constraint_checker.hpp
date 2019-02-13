#pragma once

#include <boost/container/small_vector.hpp>
#include <optional>
#include <string>

#include "storage/base_segment.hpp"
#include "storage/constraints/row_templated_constraint_checker.hpp"
#include "storage/segment_accessor.hpp"

namespace opossum {

// Each tuple of a unique constraint row is a boost small_vector with three elements already
// preallocated on the stack. Why three? We think that most constraints use a maximum of three columns.
typedef boost::container::small_vector<AllTypeVariant, 3> tuple_row;

class ConcatenatedConstraintChecker : public RowTemplatedConstraintChecker<tuple_row> {
 public:
  ConcatenatedConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : RowTemplatedConstraintChecker<tuple_row>(table, constraint) {}

  virtual std::shared_ptr<std::vector<tuple_row>> get_inserted_rows(
      std::shared_ptr<const Table> table_to_insert) const {
    auto rows = std::make_shared<std::vector<tuple_row>>();

    std::vector<std::shared_ptr<BaseSegment>> segments;

    for (const auto& chunk : table_to_insert->chunks()) {
      segments.clear();
      for (const auto column_id : this->_constraint.columns) {
        segments.emplace_back(chunk->segments()[column_id]);
      }

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        bool contains_null = false;
        tuple_row row;
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

  virtual std::optional<tuple_row> get_row(std::shared_ptr<const Chunk> chunk, const ChunkOffset chunk_offset) const {
    auto row = tuple_row();
    row.resize(this->_segments.size());

    size_t i = 0;
    for (const auto& segment : this->_segments) {
      const auto& value = segment->operator[](chunk_offset);
      if (variant_is_null(value)) {
        return std::optional<tuple_row>();
      }
      row[i++] = value;
    }
    return row;
  }

 protected:
  std::vector<std::shared_ptr<BaseSegment>> _segments;
};

}  // namespace opossum
