#include <string>

#include "storage/constraints/unique_checker.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

bool constraint_valid_for(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  std::set<boost::container::small_vector<AllTypeVariant, 3>> unique_values;

  for (const auto& chunk : table.chunks()) {
    const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

    const auto& segments = chunk->segments();
    for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
      const auto row_tid = mvcc_data->tids[chunk_offset].load();
      const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
      const auto end_cid = mvcc_data->end_cids[chunk_offset];

      auto row = boost::container::small_vector<AllTypeVariant, 3>();
      row.reserve(constraint.columns.size());

      if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
        for (const auto& column_id : constraint.columns) {
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
          return false;
        }
      }
    continue_with_next_row:;
    }
  }
  return true;
}

bool all_constraints_valid_for(std::shared_ptr<const Table> table, const CommitID& snapshot_commit_id,
                               const TransactionID& our_tid) {
  for (const auto& constraint : table->get_unique_constraints()) {
    if (!constraint_valid_for(*table, constraint, snapshot_commit_id, our_tid)) {
      return false;
    }
  }
  return true;
}

bool all_constraints_valid_for(const std::string& table_name, const CommitID& snapshot_commit_id,
                               const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  return all_constraints_valid_for(table, snapshot_commit_id, our_tid);
}

}  // namespace opossum
