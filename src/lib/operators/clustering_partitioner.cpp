#include "clustering_partitioner.hpp"

#include <memory>
#include <string>

#include "concurrency/transaction_context.hpp"
#include "operators/validate.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/reference_segment.hpp"
#include "utils/assert.hpp"

namespace opossum {


ClusteringPartitioner::ClusteringPartitioner(const std::shared_ptr<const AbstractOperator>& referencing_table_op, std::shared_ptr<Table> table, const std::shared_ptr<Chunk> chunk, const std::vector<ClusterKey>& cluster_keys, std::map<ClusterKey, std::shared_ptr<Chunk>>& clusters, std::map<ClusterKey, std::set<ChunkID>>& chunk_ids_per_cluster)
    : AbstractReadWriteOperator{OperatorType::Clustering, referencing_table_op}, _table{table}, _chunk{chunk}, _cluster_keys{cluster_keys}, _clusters{clusters}, _chunk_ids_per_cluster{chunk_ids_per_cluster} {
      Assert(_chunk->size() == _cluster_keys.size(), "We need one cluster key for every row in the chunk.");
    }

const std::string& ClusteringPartitioner::name() const {
  static const auto name = std::string{"ClusteringPartitioner"};
  return name;
}

std::shared_ptr<const Table> ClusteringPartitioner::_on_execute(std::shared_ptr<TransactionContext> context) {
  
  return nullptr;
}

void ClusteringPartitioner::_start_new_chunk(ClusterKey cluster_key) {
  // TODO: hack: assume that no other chunk is appended between append_mutable_chunk() and getting the id of the last chunk
  const auto num_chunks_before_append = _table->chunk_count();
  _table->append_mutable_chunk();        
  const auto& last_chunk = _table->get_chunk(num_chunks_before_append);
  Assert(last_chunk, "failed to get last chunk");
  Assert(_table->chunk_count() == num_chunks_before_append + 1, "some additional chunk appeared");
  _clusters[cluster_key] = last_chunk;

  if (_chunk_ids_per_cluster.find(cluster_key) == _chunk_ids_per_cluster.end()) {
    _chunk_ids_per_cluster[cluster_key] = {};
  }
  _chunk_ids_per_cluster[cluster_key].insert(num_chunks_before_append);
}

void ClusteringPartitioner::_on_commit_records(const CommitID commit_id) {
  // all locks have been acquired by now, so just write the results

  std::cout << "ClusterPartitioner is committing with id " << commit_id << std::endl;

  for (ChunkOffset chunk_offset{0}; chunk_offset < _chunk->size(); chunk_offset++) {
    const auto cluster_key = _cluster_keys[chunk_offset];

    std::vector<AllTypeVariant> insertion_values;
    for (ColumnID column_id{0}; column_id < _chunk->column_count(); column_id++) {
      const auto segment = _chunk->get_segment(column_id);
      Assert(segment, "segment was nullptr");
      insertion_values.push_back((*segment)[chunk_offset]);
    }

    if (_clusters.find(cluster_key) == _clusters.end() || _clusters[cluster_key]->size() == _table->target_chunk_size()) {
      _start_new_chunk(cluster_key);
    }

    const auto& cluster_chunk = _clusters[cluster_key];
    const auto& new_mvcc_data = cluster_chunk->mvcc_data();
    const auto new_row_index = cluster_chunk->size();

    _chunk->mvcc_data()->set_end_cid(chunk_offset, commit_id);
    _chunk->increase_invalid_row_count(1);
    new_mvcc_data->set_begin_cid(new_row_index, commit_id);
    cluster_chunk->append(insertion_values);
  }
  Assert(_chunk->invalid_row_count() == _chunk->size(), "only " + std::to_string(_chunk->invalid_row_count()) + " of " + std::to_string(_chunk->size()) + " marked invalid");
}

// TODO unlock both on commit and rollback

void ClusteringPartitioner::_on_rollback_records() {
  
}

std::shared_ptr<AbstractOperator> ClusteringPartitioner::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  //return std::make_shared<Clustering>(copied_input_left);
  return nullptr;
}

void ClusteringPartitioner::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

}  // namespace opossum
