#pragma once

#include <algorithm>
#include <optional>
#include <string>

#include "operators/validate.hpp"
#include "storage/constraints/base_constraint_checker.hpp"
#include "storage/segment_accessor.hpp"

namespace opossum {

template <typename T>
class SingleConstraintChecker : public BaseConstraintChecker {
 public:
  SingleConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : BaseConstraintChecker(table, constraint) {
    Assert(constraint.columns.size() == 1, "Only one column constraints allowed for SingleConstraintChecker");

    // T is the "row" type (like actual row type is column type T or T=boost_small_vector<AllTypeVariant, 3>)
    // function getInsertedValues (table) -> std::vector<T>
    // function: gather row (chunk, chunk_offset, segment_accessors) -> optional<T>
    //    (optional empty means that row (only constrained values) contains a NULL and row check can be skipped)
  }

  virtual std::tuple<bool, ChunkID> isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert,
                                                             const CommitID snapshot_commit_id,
                                                             const TransactionID our_tid, const ChunkID since) {
    auto values_to_insert = getInsertedValues(table_to_insert);
    std::sort(values_to_insert->begin(), values_to_insert->end());

    bool only_one = values_to_insert->size() == 1;
    T only_one_value = T();
    if (only_one) {
      only_one_value = values_to_insert->operator[](0);
    }

    ChunkID first_value_segment{0};
    bool first_value_segment_set = false;

    ChunkID chunk_id{0};
    for (const auto& chunk : _table.chunks()) {
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      /*
      if (chunk_id < since) {
        chunk_id++;
        continue;
      }
      chunk_id++;
      */

      const auto segment = chunk->segments()[_constraint.columns[0]];
      bool is_value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(segment);
      if (is_value_segment && !first_value_segment_set) {
        first_value_segment = chunk_id;
        first_value_segment_set = true;
      }
      const auto segment_accessor = create_segment_accessor<T>(segment);

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          std::optional<T> value = segment_accessor->access(chunk_offset);
          // Since null values are considered unique (Assert(null != null)), we skip a row if we encounter a null value.
          if (!value.has_value()) {
            continue;
          }

          bool found = only_one ? value.value() == only_one_value
                                : std::binary_search(values_to_insert->begin(), values_to_insert->end(), value.value());
          if (found) {
            return std::make_tuple<>(false, first_value_segment);
          }
        }
      }
    }
    return std::make_tuple<>(true, first_value_segment);
  }

  std::shared_ptr<std::vector<T>> getInsertedValues(std::shared_ptr<const Table> table_to_insert) const {
    auto values = std::make_shared<std::vector<T>>();

    for (const auto& chunk : table_to_insert->chunks()) {
      const auto segment = chunk->segments()[_constraint.columns[0]];
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

  virtual std::tuple<bool, ChunkID> isValid(const CommitID snapshot_commit_id, const TransactionID our_tid) {
    std::set<T> unique_values;

    for (const auto& chunk : _table.chunks()) {
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      const auto segment = chunk->segments()[_constraint.columns[0]];
      const auto segment_accessor = create_segment_accessor<T>(segment);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          std::optional<T> value = segment_accessor->access(chunk_offset);
          // Since null values are considered unique (Assert(null != null)), we skip a row if we encounter a null value.
          if (!value.has_value()) {
            goto continue_with_next_row;
          }

          const auto& [iterator, inserted] = unique_values.insert(value.value());
          if (!inserted) {
            return std::make_tuple<>(false, ChunkID{0});
          }
        }
        {
        continue_with_next_row:;
        }
      }
    }
    return std::make_tuple<>(true, ChunkID{0});
  }
};

}  // namespace opossum
