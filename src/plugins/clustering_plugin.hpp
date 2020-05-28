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
  ClusteringPlugin() {}

  const std::string description() const final;

  void start() final;

  void stop() final;

  static const ClusteringByTable read_clustering_config(const std::string& filename = "clustering_config.json");

 private:
  void _write_clustering_information() const;

  ClusteringByTable _clustering_config;
  std::shared_ptr<AbstractClusteringAlgo> _clustering_algo;
};

}  // namespace opossum
