#pragma once

#include <memory>
#include <utility>
#include <string>

#include "abstract_clustering_algo.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

class ChunkwiseClusteringAlgo : public AbstractClusteringAlgo {
 public:
  // TODO sm
  ChunkwiseClusteringAlgo(StorageManager& storage_manager, ClusteringByTable clustering);

  const std::string description() const override;  

 protected:
  
  void _perform_clustering() override;

  template <typename ColumnDataType>
  std::shared_ptr<const AbstractHistogram<ColumnDataType>> _get_histogram(const std::shared_ptr<const Table>& table, const std::string& column_name) const;

  template <typename ColumnDataType>
  std::vector<std::pair<ColumnDataType, ColumnDataType>> _get_boundaries(const std::shared_ptr<const AbstractHistogram<ColumnDataType>>& histogram, const size_t row_count, const size_t split_factor, const size_t rows_per_chunk) const;  
};

} // namespace opossum