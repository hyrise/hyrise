#pragma once

#include <memory>
#include <utility>
#include <string>

#include "abstract_clustering_algo.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class DisjointClustersAlgo : public AbstractClusteringAlgo {
 public:

  DisjointClustersAlgo(ClusteringByTable clustering);

  const std::string description() const override;

 protected:
  
  void _perform_clustering() override;

  template <typename ColumnDataType>
  std::vector<std::pair<AllTypeVariant, AllTypeVariant>> _get_boundaries(const std::shared_ptr<const AbstractHistogram<ColumnDataType>>& histogram, const size_t row_count, const size_t num_clusters) const;

  std::vector<std::shared_ptr<Chunk>> _distribute_chunk(const std::shared_ptr<Chunk>& chunk, const std::shared_ptr<Table>& table, const std::vector<std::vector<std::pair<AllTypeVariant, AllTypeVariant>>>& boundaries, std::vector<std::shared_ptr<Chunk>>& partially_filled_chunks, const std::vector<std::shared_ptr<Chunk>>& previously_partially_filled_chunks, const std::vector<ColumnID>& clustering_column_ids);

  std::vector<std::shared_ptr<Chunk>> _sort_and_encode_chunks(const std::vector<std::shared_ptr<Chunk>>& chunks, const ColumnID sort_column_id, const std::shared_ptr<Table>& table) const;  
};

} // namespace opossum