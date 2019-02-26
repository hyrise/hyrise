#pragma once

#include <algorithm>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/constraints/base_constraint_checker.hpp"
#include "storage/table.hpp"

namespace opossum {

/**
 * Implements checking constraints with the help of a few more templated virtual functions.
 *
 * The template argument is the type of a row that should be unique. This can be the data
 * type of a single column for a constraint with only one column (see SingleConstraintChecker)
 * or a vector with AllTypeVariant's (see ConcatenatedConstraintChecker). The virtual
 * functions are used to fetch these rows from the table and then to make sure that they are unique.
 */
template <typename Row>
class RowTemplatedConstraintChecker : public BaseConstraintChecker {
 public:
  RowTemplatedConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : BaseConstraintChecker(table, constraint) {}

  /**
   * Extracts the rows that are being inserted from the table given to the insert operator.
   */
  virtual std::shared_ptr<std::vector<Row>> get_inserted_rows(std::shared_ptr<const Table> table) const = 0;

  /**
   * Prepare for returning values when get_row is called.
   * Return false if the segment doesn't need to be checked for duplicate values.
   */
  virtual bool preprocess_chunk(std::shared_ptr<const Chunk> chunk) { return true; }

  /**
   * Returns a row from the current chunk (pre_process_chunk is called before) at the given chunk offset.
   * Should return a null optional if the row is null or contains a null, i.e. no constraint check is required.
   */
  virtual std::optional<Row> get_row(std::shared_ptr<const Chunk> chunk, const ChunkOffset chunk_offset) const = 0;

  virtual std::tuple<bool, ChunkID> is_valid(const CommitID snapshot_commit_id, const TransactionID our_tid) {
    _values_to_insert = nullptr;

    std::set<Row> unique_values;

    for (const auto& chunk : this->_table.chunks()) {
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      preprocess_chunk(chunk);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          std::optional<Row> row = get_row(chunk, chunk_offset);
          if (!row.has_value()) {
            continue;
          }

          const auto& [iterator, inserted] = unique_values.insert(row.value());
          if (!inserted) {
            return std::make_tuple<>(false, MAX_CHUNK_ID);
          }
        }
      }
    }
    return std::make_tuple<>(true, MAX_CHUNK_ID);
  }

  virtual std::tuple<bool, ChunkID> is_valid_for_inserted_values(std::shared_ptr<const Table> table_to_insert,
                                                                 const CommitID snapshot_commit_id,
                                                                 const TransactionID our_tid,
                                                                 const ChunkID start_chunk_id) {
    // We store all rows to be inserted in an ordered vector and check for each row in the table
    // with a binary search if that row is contained in the inserted values.
    // In almost all cases (different number of inserted values) this is faster than using a set or unordered_set:
    // http://quick-bench.com/005z63fU43ivxWQsl9Fm_P8zu0k
    _values_to_insert = get_inserted_rows(table_to_insert);
    std::sort(_values_to_insert->begin(), _values_to_insert->end());

    // Also, if there is only a single row to be inserted we keep it stored directly for faster access.
    // (Benchmarked with the same benchmark above.)
    std::optional<Row> single_insert_value{};
    if (_values_to_insert->size() == 1) {
      single_insert_value = (*_values_to_insert)[0];
    }

    // We remember and return the id of the first mutable chunk.
    // Reason is that we can skip checking compressed chunks during the commit as they have been checked
    // already with the operator and won't change later.
    std::optional<ChunkID> first_mutable_chunk{};

    for (ChunkID chunk_id{0}; chunk_id < this->_table.chunk_count(); chunk_id++) {
      const auto& chunk = this->_table.get_chunk(chunk_id);
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      if (chunk_id < start_chunk_id) {
        continue;
      }

      if (chunk->is_mutable() && !first_mutable_chunk.has_value()) {
        first_mutable_chunk = chunk_id;
      }

      if (!preprocess_chunk(chunk)) {
        continue;
      }

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          std::optional<Row> row = get_row(chunk, chunk_offset);
          // If a row contains a null, it can be skipped from unique checking entirely
          // because a null can stand for any value is thus is always unique.
          if (!row.has_value()) {
            continue;
          }

          bool found = single_insert_value.has_value()
                           ? row.value() == single_insert_value.value()
                           : std::binary_search(_values_to_insert->begin(), _values_to_insert->end(), row.value());
          if (found) {
            return std::make_tuple<>(false, first_mutable_chunk.value_or(MAX_CHUNK_ID));
          }
        }
      }
    }
    return std::make_tuple<>(true, first_mutable_chunk.value_or(MAX_CHUNK_ID));
  }

 protected:
  std::shared_ptr<std::vector<Row>> _values_to_insert;
};

}  // namespace opossum
