#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * An operator that uses Mvcc to "replace" a set of chunks with their sorted version.
 */
class ClusteringSorter : public AbstractReadWriteOperator {
 public:
  explicit ClusteringSorter(const std::shared_ptr<const AbstractOperator>& referencing_table_op,
                            const std::shared_ptr<Table> table, const std::set<ChunkID>& chunk_ids,
                            const ColumnID sort_column_id, std::unordered_set<ChunkID>& new_chunk_ids);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right,
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID commit_id) override;
  void _on_rollback_records() override;

 private:
  bool _lock_chunk(const std::shared_ptr<Chunk> chunk);
  void _unlock_chunk(const std::shared_ptr<Chunk> chunk);
  void _unlock_all();

  const std::shared_ptr<Table> _table;
  const std::set<ChunkID> _chunk_ids;
  const ColumnID _sort_column_id;
  std::unordered_set<ChunkID>& _new_chunk_ids;

  size_t _num_locks;
  size_t _expected_num_locks;
  TransactionID _transaction_id;
  std::shared_ptr<const Table> _sorted_table;
};
}  // namespace opossum
