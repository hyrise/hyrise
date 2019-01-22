#include <string>
#include <optional>

#include "concurrency/transaction_manager.hpp"
#include "storage/constraints/unique_checker.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class BaseConstraintChecker {
public:
  BaseConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
    : _table(table), _constraint(constraint) {
  }
  virtual ~BaseConstraintChecker() = default;

  virtual bool isValid(const CommitID& snapshot_commit_id, const TransactionID& our_tid) = 0;
  virtual bool isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert, const CommitID& snapshot_commit_id, const TransactionID& our_tid) = 0;

protected:
  const Table& _table;
  const TableConstraintDefinition& _constraint;
};

template <typename T>
class SingleConstraintChecker : public BaseConstraintChecker {
public:
  SingleConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
    : BaseConstraintChecker(table, constraint) {

      Assert(constraint.columns.size() == 1, "Only one column constraints allowed for SingleConstraintChecker");
  }

  virtual bool isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
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

          if (values_to_insert->find(value.value()) != values_to_insert->end()) {
            return false;
          }
        }
  {continue_with_next_row:;}
      }
    }
    return true;
  }

  std::shared_ptr<std::set<T>> getInsertedValues(std::shared_ptr<const Table> table_to_insert) const {
    std::set<T> values;

    for (const auto& chunk : table_to_insert->chunks()) {
      const auto segment = chunk->segments()[_constraint.columns[0]];
      const auto segment_accessor = create_segment_accessor<T>(segment);
      for (ChunkOffset chunk_offset = 0; chunk_offset < chunk->size(); chunk_offset++) {
        std::optional<T> value = segment_accessor->access(chunk_offset);
        if (value.has_value()) {
          values.insert(value.value());
        }
      }
    }
    return std::make_shared<std::set<T>>();
  }

  virtual bool isValid(const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
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

class ConcatenatedConstraintChecker : public BaseConstraintChecker {
public:
  ConcatenatedConstraintChecker(const Table& table, const TableConstraintDefinition& constraint)
    : BaseConstraintChecker(table, constraint) {
  }

  std::shared_ptr<std::set<boost::container::small_vector<AllTypeVariant, 3>>> getInsertedValues(std::shared_ptr<const Table> table_to_insert) const {
    return std::make_shared<std::set<boost::container::small_vector<AllTypeVariant, 3>>>();
  }

  virtual bool isValidForInsertedValues(std::shared_ptr<const Table> table_to_insert, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
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
            return false;
          }
        }
  {continue_with_next_row:;}
      }
    }
    return true;
  }

  virtual bool isValid(const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
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
            return false;
          }
        }
  {continue_with_next_row:;}
      }
    }
    return true;
  }
};

std::shared_ptr<BaseConstraintChecker> create_constraint_checker(const Table& table, const TableConstraintDefinition& constraint) {
  if (constraint.columns.size() == 1) {
    ColumnID column_id = constraint.columns[0];
    DataType data_type = table.column_data_type(column_id);
    return make_shared_by_data_type<BaseConstraintChecker, SingleConstraintChecker>(data_type, table, constraint);
  } else {
    return std::make_shared<ConcatenatedConstraintChecker>(table, constraint);
  }
}

bool constraint_valid_for(const Table& table, const TableConstraintDefinition& constraint,
                          const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  const auto checker = create_constraint_checker(table, constraint);
  return checker->isValid(snapshot_commit_id, our_tid);
}

bool all_constraints_valid_for(const std::string& table_name, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  const auto table = StorageManager::get().get_table(table_name);
  for (const auto& constraint : table->get_unique_constraints()) {
    const auto checker = create_constraint_checker(*table, constraint);
    if (!checker->isValid(snapshot_commit_id, our_tid)) {
      return false;
    }
  }
  return true;
}

bool all_constraints_valid_for(std::shared_ptr<const Table> table, std::shared_ptr<const Table> table_to_insert, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  for (const auto& constraint : table->get_unique_constraints()) {
    const auto checker = create_constraint_checker(*table, constraint);
    if (!checker->isValidForInsertedValues(table_to_insert, snapshot_commit_id, our_tid)) {
      return false;
    }
  }
  return true;
}

bool all_constraints_valid_for(const std::string& table_name, std::shared_ptr<const Table> table_to_insert,
                               const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  return all_constraints_valid_for(table, table_to_insert, snapshot_commit_id, our_tid);
}

bool check_constraints_in_commit_range(const std::string& table_name, std::vector<std::shared_ptr<const Table>> tables_to_insert, const CommitID& begin_snapshot_commit_id, const CommitID& end_snapshot_commit_id,
                               const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  return false;
  //return all_constraints_valid_for(table, end_snapshot_commit_id, our_tid);
}

bool check_constraints_for_values(const std::string& table_name, std::shared_ptr<const Table> table_to_insert, const CommitID& snapshot_commit_id, const TransactionID& our_tid) {
  auto const table = StorageManager::get().get_table(table_name);
  return all_constraints_valid_for(table, table_to_insert, snapshot_commit_id, our_tid);
} 

}  // namespace opossum
