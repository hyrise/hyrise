#pragma once

#include <string>
#include <optional>

#include "storage/segment_accessor.hpp"
#include "storage/constraints/base_constraint_checker.hpp"

namespace opossum {

template <typename T>
class SingleConstraintChecker : public BaseConstraintChecker {
public:
  SingleConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
    : BaseConstraintChecker(table, constraint) {

      Assert(constraint.columns.size() == 1, "Only one column constraints allowed for SingleConstraintChecker");
  }

  virtual bool isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert, const CommitID snapshot_commit_id, const TransactionID our_tid) {
    auto values_to_insert = getInsertedValues(table_to_insert);

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

          if (values_to_insert->count(value.value())) {
            return false;
          }
        }
  {continue_with_next_row:;}
      }
    }
    return true;
  }

  std::shared_ptr<std::set<T>> getInsertedValues(std::shared_ptr<const Table> table_to_insert) const {
    auto values = std::make_shared<std::set<T>>();

    for (const auto& chunk : table_to_insert->chunks()) {
      const auto segment = chunk->segments()[_constraint.columns[0]];
      const auto segment_accessor = create_segment_accessor<T>(segment);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        std::optional<T> value = segment_accessor->access(chunk_offset);
        if (value.has_value()) {
          values->insert(value.value());
        }
      }
    }
    return values;
  }

  virtual bool isValid(const CommitID snapshot_commit_id, const TransactionID our_tid) {
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
            return false;
          }
        }
  {continue_with_next_row:;}
      }
    }
    return true;
  }
};

}  // namespace opossum
