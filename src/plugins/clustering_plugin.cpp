#include "clustering_plugin.hpp"

#include <iostream>
#include <filesystem>
#include <fstream>

#include "clustering/abstract_clustering_algo.hpp"
#include "clustering/simple_clustering_algo.hpp"
#include "nlohmann/json.hpp"

namespace opossum {

const std::string ClusteringPlugin::description() const { return "This is the Hyrise ClusteringPlugin"; }

void ClusteringPlugin::start() {
  const auto clustering_config = _read_clustering_config();
  auto clustering_algo = SimpleClusteringAlgo(storage_manager, clustering_config);

  std::cout << "[ClusteringPlugin] Starting clustering, using " << clustering_algo.description() << std::endl;

  clustering_algo.run();

  std::cout << "[ClusteringPlugin] Clustering complete." << std::endl;
}

void ClusteringPlugin::stop() { }

const ClusteringByTable ClusteringPlugin::_read_clustering_config(const std::string& filename) const {
  if (!std::filesystem::exists(filename)) {
    std::cout << "clustering config file not found: " << filename << std::endl;
    std::exit(1);
  } else {
    std::cout << "clustering file exists" << std::endl;
  }

  std::ifstream ifs(filename);
  const auto clustering_config = nlohmann::json::parse(ifs);
  return clustering_config;
}

EXPORT_PLUGIN(ClusteringPlugin)

}  // namespace opossum
