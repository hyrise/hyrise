#pragma once

#include <optional>
#include <string>

#include "storage/constraints/base_constraint_checker.hpp"
#include "storage/segment_accessor.hpp"

namespace opossum {

class ConcatenatedConstraintChecker : public BaseConstraintChecker {
 public:
  ConcatenatedConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : BaseConstraintChecker(table, constraint) {}

  std::shared_ptr<std::set<boost::container::small_vector<AllTypeVariant, 3>>> getInsertedValues(
      std::shared_ptr<const Table> table_to_insert) const {
    return std::make_shared<std::set<boost::container::small_vector<AllTypeVariant, 3>>>();
  }

  virtual std::tuple<bool, ChunkID> isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert,
                                                             const CommitID snapshot_commit_id,
                                                             const TransactionID our_tid, const ChunkID since) {
    auto values_to_insert = getInsertedValues(table_to_insert);

    for (const auto& chunk : _table.chunks()) {
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      const auto& segments = chunk->segments();
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        auto row = boost::container::small_vector<AllTypeVariant, 3>();
        row.reserve(_constraint.columns.size());

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          for (const auto& column_id : _constraint.columns) {
            const auto& segment = segments[column_id];
            const auto& value = segment->operator[](chunk_offset);
            // Since null values are considered unique (Assert(null != null)), we skip a row if we encounter a null value.
            if (variant_is_null(value)) {
              goto continue_with_next_row;
            }
            row.emplace_back(value);
          }

          if (values_to_insert->find(row) != values_to_insert->end()) {
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

  virtual std::tuple<bool, ChunkID> isValid(const CommitID snapshot_commit_id, const TransactionID our_tid) {
    std::set<boost::container::small_vector<AllTypeVariant, 3>> unique_values;

    for (const auto& chunk : _table.chunks()) {
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      const auto& segments = chunk->segments();
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        auto row = boost::container::small_vector<AllTypeVariant, 3>();
        row.reserve(_constraint.columns.size());

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          for (const auto& column_id : _constraint.columns) {
            const auto& segment = segments[column_id];
            const auto& value = segment->operator[](chunk_offset);
            // Since null values are considered unique (Assert(null != null)), we skip a row if we encounter a null value.
            if (variant_is_null(value)) {
              goto continue_with_next_row;
            }
            row.emplace_back(value);
          }

          const auto& [iterator, inserted] = unique_values.insert(row);
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
