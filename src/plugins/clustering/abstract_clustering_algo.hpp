#pragma once

#include <string>
#include <map>
#include <vector>
#include <utility>

#include "storage/storage_manager.hpp"

namespace opossum {

using ClusteringByTable = std::map<std::string, std::vector<std::pair<std::string, uint32_t>>>;
class AbstractClusteringAlgo {
 public:

  AbstractClusteringAlgo(StorageManager& storage_manager, ClusteringByTable clustering) : storage_manager(storage_manager), clustering_by_table(clustering) {}

  virtual ~AbstractClusteringAlgo() = default;

  virtual const std::string description() const = 0;

  void run();

  StorageManager& storage_manager;
  ClusteringByTable clustering_by_table;

 protected:
  void _run_assertions() const;
  virtual void _perform_clustering() = 0;

  std::map<std::string, size_t> _original_table_sizes;
};

}  // namespace opossum
