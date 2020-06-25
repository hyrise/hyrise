#include "clustering_partitioner.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {


ClusteringPartitioner::ClusteringPartitioner(const std::shared_ptr<const AbstractOperator>& referencing_table_op, std::shared_ptr<Table> table, const std::shared_ptr<Chunk> chunk, const std::vector<ClusterKey>& cluster_keys, const size_t expected_invalid_row_count, std::map<ClusterKey, std::pair<ChunkID, std::shared_ptr<Chunk>>>& clusters, std::map<ClusterKey, std::set<ChunkID>>& chunk_ids_per_cluster)
    : AbstractReadWriteOperator{OperatorType::ClusteringPartitioner, referencing_table_op}, _table{table}, _chunk{chunk}, _cluster_keys{cluster_keys}, _expected_invalid_row_count{expected_invalid_row_count}, _clusters{clusters}, _chunk_ids_per_cluster{chunk_ids_per_cluster}, _num_locks{0}, _transaction_id{0} {
      Assert(_chunk->size() == _cluster_keys.size(), "We need one cluster key for every row in the chunk.");
    }

const std::string& ClusteringPartitioner::name() const {
  static const auto name = std::string{"ClusteringPartitioner"};
  return name;
}

void ClusteringPartitioner::_unlock_chunk(const std::shared_ptr<Chunk> chunk) {
  const auto mvcc_data = chunk->mvcc_data();

  for (ChunkOffset offset{0}; offset < chunk->size(); offset++) {
    if (mvcc_data->get_tid(offset) == _transaction_id) {
      const auto success = mvcc_data->compare_exchange_tid(offset, _transaction_id, 0u);
      Assert(success, "Unable to unlock a row that belongs to our own transaction");
      _num_locks--;
    }
  }

  Assert(_num_locks == 0, "there should be no more locks, but got " + std::to_string(_num_locks));
}

std::shared_ptr<const Table> ClusteringPartitioner::_on_execute(std::shared_ptr<TransactionContext> context) {
  const auto mvcc_data = _chunk->mvcc_data();
  _transaction_id = context->transaction_id();

  for (ChunkOffset offset{0}; offset < _chunk->size(); offset++) {
    if (mvcc_data->get_end_cid(offset) != MvccData::MAX_COMMIT_ID) {
      // The DELETE operator locks rows to delete them, but does not unlock on commit. TODO Bug or feature?
      continue;
    }

    const auto expected = 0u;
    auto success = mvcc_data->compare_exchange_tid(offset, expected, _transaction_id);
    if (!success) {
      _mark_as_failed();
      return nullptr;
    } else {
      _num_locks++;
    }
  }

  if (_chunk->invalid_row_count() != _expected_invalid_row_count) {
    Assert(_chunk->invalid_row_count() > _expected_invalid_row_count, "invalid row count cannot decrease");
    _mark_as_failed();
  }

  return nullptr;
}

void ClusteringPartitioner::_start_new_chunk(ClusterKey cluster_key) {
  const auto append_lock = _table->acquire_append_mutex();
  _table->append_mutable_chunk();        
  const auto& last_chunk = _table->last_chunk();
  Assert(last_chunk, "failed to get last chunk");
  const ChunkID appended_chunk_id {_table->chunk_count() - 1};
  _clusters[cluster_key] = std::make_pair(appended_chunk_id, last_chunk);

  if (_chunk_ids_per_cluster.find(cluster_key) == _chunk_ids_per_cluster.end()) {
    _chunk_ids_per_cluster[cluster_key] = {};
  }
  _chunk_ids_per_cluster[cluster_key].insert(appended_chunk_id);
}

void ClusteringPartitioner::_on_commit_records(const CommitID commit_id) {
  // all locks have been acquired by now, so just write the results
  const auto mvcc_data = _chunk->mvcc_data();

  for (ChunkOffset chunk_offset{0}; chunk_offset < _cluster_keys.size(); chunk_offset++) {
    if (mvcc_data->get_end_cid(chunk_offset) != MvccData::MAX_COMMIT_ID) {
      // Row is already marked as deleted. Do not cluster it.
      continue;
    }

    const auto cluster_key = _cluster_keys[chunk_offset];

    std::vector<AllTypeVariant> insertion_values;
    for (ColumnID column_id{0}; column_id < _chunk->column_count(); column_id++) {
      const auto segment = _chunk->get_segment(column_id);
      Assert(segment, "segment was nullptr");
      insertion_values.push_back((*segment)[chunk_offset]);
    }

    if (_clusters.find(cluster_key) == _clusters.end() || _clusters[cluster_key].second->size() == _table->target_chunk_size()) {
      _start_new_chunk(cluster_key);
    }

    const auto& cluster_chunk = _clusters[cluster_key].second;
    const auto& new_mvcc_data = cluster_chunk->mvcc_data();
    const auto new_row_index = cluster_chunk->size();

    _chunk->mvcc_data()->set_end_cid(chunk_offset, commit_id);
    _chunk->increase_invalid_row_count(1);
    new_mvcc_data->set_begin_cid(new_row_index, commit_id);
    cluster_chunk->append(insertion_values);
  }

  Assert(_chunk->invalid_row_count() == _chunk->size(), "only " + std::to_string(_chunk->invalid_row_count()) + " of " + std::to_string(_chunk->size()) + " marked invalid");
  _chunk->set_cleanup_commit_id(commit_id);
  _unlock_chunk(_chunk);
}

// TODO do we need locks on the cluster-chunks that are added by this operator?

void ClusteringPartitioner::_on_rollback_records() {
  _unlock_chunk(_chunk);
}

std::shared_ptr<AbstractOperator> ClusteringPartitioner::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  //return std::make_shared<Clustering>(copied_input_left);
  return nullptr;
}

void ClusteringPartitioner::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
