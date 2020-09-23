#pragma once

#include <memory>
#include <utility>
#include <string>

#include "abstract_clustering_algo.hpp"
#include "operators/clustering_partitioner.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

using ClusterBoundary = std::pair<AllTypeVariant, AllTypeVariant>;
using ClusterBoundaries = std::vector<ClusterBoundary>;

class DisjointClustersAlgo : public AbstractClusteringAlgo {
 public:

  DisjointClustersAlgo(ClusteringByTable clustering);

  const std::string description() const override;

 protected:
  
  void _perform_clustering() override;

  template <typename ColumnDataType>
  ClusterBoundaries _get_boundaries(const std::shared_ptr<const AbstractHistogram<ColumnDataType>>& histogram, const size_t row_count, const size_t num_clusters, const bool nullable) const;

  std::vector<ClusterBoundaries> _all_cluster_boundaries(const std::vector<size_t>& num_clusters_per_dimension) const;

  const ClusterKey _clustering_key_for_chunk(const std::shared_ptr<Chunk>& chunk) const;
  const std::vector<ClusterKey> _cluster_keys(const std::shared_ptr<Chunk>& chunk) const;

  std::vector<std::shared_ptr<Chunk>> _sort_and_encode_chunks(const std::vector<std::shared_ptr<Chunk>>& chunks, const ColumnID sort_column_id) const;

  bool _can_delete_chunk(const std::shared_ptr<Chunk> chunk) const;

  void _print_boundary_counts(const std::string& column_name) const;

 private:
  std::vector<ClusterBoundaries> _boundaries;
  std::vector<ColumnID> _clustering_column_ids;
  std::shared_ptr<Table> _table;
};

} // namespace opossum