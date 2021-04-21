#pragma once

#include <memory>
#include <string>
#include <utility>

#include "abstract_clustering_algo.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class ChunkwiseClusteringAlgo : public AbstractClusteringAlgo {
 public:
  // TODO sm
  ChunkwiseClusteringAlgo(ClusteringByTable clustering);

  const std::string description() const override;

 protected:
  void _perform_clustering() override;

  template <typename ColumnDataType>
  std::vector<std::pair<ColumnDataType, ColumnDataType>> _get_boundaries(
      const std::shared_ptr<const AbstractHistogram<ColumnDataType>>& histogram, const size_t row_count,
      const size_t split_factor, const size_t rows_per_chunk) const;
};

}  // namespace opossum