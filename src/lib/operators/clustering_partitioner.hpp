#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "abstract_read_write_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

using ClusterKey = std::vector<size_t>;

/**
 * An operator that takes a chunk and clustering keys for its rows.
 * The operator "moves" each row (via Mvcc) to corresponding "cluster-chunk".
 */
class ClusteringPartitioner : public AbstractReadWriteOperator {
 public:
  explicit ClusteringPartitioner(const std::shared_ptr<const AbstractOperator>& referencing_table_op, const std::shared_ptr<Table> table, const std::shared_ptr<Chunk> chunk, const std::vector<ClusterKey>& cluster_keys, std::map<ClusterKey, std::pair<ChunkID, std::shared_ptr<Chunk>>>& clusters, std::map<ClusterKey, std::set<ChunkID>>& _chunk_ids_per_cluster);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute(std::shared_ptr<TransactionContext> context) override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;
  void _on_commit_records(const CommitID commit_id) override;
  void _on_rollback_records() override;

 private:  
  void _start_new_chunk(ClusterKey cluster_key);
  void _unlock_chunk(const std::shared_ptr<Chunk> chunk);

  const std::shared_ptr<Table> _table;
  const std::shared_ptr<Chunk> _chunk;
  const std::vector<ClusterKey>& _cluster_keys;

  std::map<ClusterKey, std::pair<ChunkID, std::shared_ptr<Chunk>>>& _clusters;
  std::map<ClusterKey, std::set<ChunkID>>& _chunk_ids_per_cluster;

  size_t _num_locks;
  TransactionID _transaction_id;
};
}  // namespace opossum
