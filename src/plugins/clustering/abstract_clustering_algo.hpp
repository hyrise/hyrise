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

  virtual void run() = 0;

  StorageManager& storage_manager;
  ClusteringByTable clustering_by_table;
};

}  // namespace opossum
