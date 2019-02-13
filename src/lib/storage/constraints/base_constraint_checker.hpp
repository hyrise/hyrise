#pragma once

#include <optional>
#include <string>
#include <tuple>

#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

class BaseConstraintChecker {
 public:
  BaseConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : _table(table), _constraint(constraint) {}
  virtual ~BaseConstraintChecker() = default;

  virtual std::tuple<bool, ChunkID> is_valid(const CommitID snapshot_commit_id, const TransactionID our_tid) = 0;
  virtual std::tuple<bool, ChunkID> is_valid_for_inserted_values(std::shared_ptr<const Table> table_to_insert,
                                                             const CommitID snapshot_commit_id,
                                                             const TransactionID our_tid, const ChunkID since) = 0;

 protected:
  const Table& _table;
  const TableConstraintDefinition& _constraint;
};

template <typename Row>
class BaseBaseConstraintChecker : public BaseConstraintChecker {
 public:
  BaseBaseConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
      : BaseConstraintChecker(table, constraint) {}

  virtual std::shared_ptr<std::vector<Row>> get_inserted_rows(std::shared_ptr<const Table> table) const = 0;

  /**
   * Prepare for returning values when get_row is called.
   * Return false if the segment doesn't need to be checked for duplicate values.
   * TODO(pk_group): naming
   */
  virtual bool prepare_read_chunk(std::shared_ptr<const Chunk> chunk) {
    return true;
  };
  virtual std::optional<Row> get_row(std::shared_ptr<const Chunk> chunk, const ChunkOffset chunk_offset) const = 0;

  virtual std::tuple<bool, ChunkID> is_valid_for_inserted_values(std::shared_ptr<const Table> table_to_insert,
                                                             const CommitID snapshot_commit_id,
                                                             const TransactionID our_tid, const ChunkID since) {
    _values_to_insert = get_inserted_rows(table_to_insert);
    std::sort(_values_to_insert->begin(), _values_to_insert->end());

    bool only_one = _values_to_insert->size() == 1;
    Row only_one_value = Row();
    if (only_one) {
      only_one_value = _values_to_insert->operator[](0);
    }

    ChunkID first_value_segment{0};
    bool first_value_segment_set = false;

    ChunkID chunk_id{0};
    for (const auto& chunk : this->_table.chunks()) {
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      if (chunk_id < since) {
        chunk_id++;
        continue;
      }
      chunk_id++;

      if (chunk->is_mutable() && !first_value_segment_set) {
        first_value_segment = chunk_id;
        first_value_segment_set = true;
      }

      if (!prepare_read_chunk(chunk)) {
        continue;
      }

      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          std::optional<Row> row = get_row(chunk, chunk_offset);
          // Since null values are considered unique (Assert(null != null)), we skip a row if we encounter a null value.
          if (!row.has_value()) {
            continue;
          }

          bool found = only_one ? row.value() == only_one_value
                                : std::binary_search(_values_to_insert->begin(), _values_to_insert->end(), row.value());
          if (found) {
            return std::make_tuple<>(false, first_value_segment);
          }
        }
      }
    }
    return std::make_tuple<>(true, first_value_segment);
  }

  virtual std::tuple<bool, ChunkID> is_valid(const CommitID snapshot_commit_id, const TransactionID our_tid) {
    _values_to_insert = nullptr;

    std::set<Row> unique_values;

    for (const auto& chunk : this->_table.chunks()) {
      const auto mvcc_data = chunk->get_scoped_mvcc_data_lock();

      prepare_read_chunk(chunk);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        const auto row_tid = mvcc_data->tids[chunk_offset].load();
        const auto begin_cid = mvcc_data->begin_cids[chunk_offset];
        const auto end_cid = mvcc_data->end_cids[chunk_offset];

        if (Validate::is_row_visible(our_tid, snapshot_commit_id, row_tid, begin_cid, end_cid)) {
          std::optional<Row> row = get_row(chunk, chunk_offset);
          if (!row.has_value()) {
            goto continue_with_next_row;
          }

          const auto& [iterator, inserted] = unique_values.insert(row.value());
          if (!inserted) {
            return std::make_tuple<>(false, ChunkID{0});
          }
        }
        {continue_with_next_row:;}
      }
    }
    return std::make_tuple<>(true, ChunkID{0});
  }

 protected:
  std::shared_ptr<std::vector<Row>> _values_to_insert;

};

}  // namespace opossum
