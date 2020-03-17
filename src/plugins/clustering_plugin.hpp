#pragma once

#include <string>

#include "hyrise.hpp"

#include "clustering/abstract_clustering_algo.hpp"
#include "storage/storage_manager.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/singleton.hpp"

namespace opossum {

class ClusteringPlugin : public AbstractPlugin {
 public:
  ClusteringPlugin() : storage_manager(Hyrise::get().storage_manager) {}

  const std::string description() const final;

  void start() final;

  void stop() final;

  StorageManager& storage_manager;

 protected:
  const ClusteringByTable _read_clustering_config(const std::string& filename = "clustering_config.json") const;
};

}  // namespace opossum
