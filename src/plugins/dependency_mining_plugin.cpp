#include "dependency_mining_plugin.hpp"

#include "hyrise.hpp"

namespace opossum {

std::string DependencyMiningPlugin::description() const { return "Dependency Mining Plugin"; }

void DependencyMiningPlugin::start() {
  _queue = std::make_shared<tbb::concurrent_priority_queue<DependencyCandidate>>();
  _pqp_analyzer.set_queue(_queue);
  _dependency_validator.set_queue(_queue);
  std::cout << "====================================================\nStarting Dependency Mining Plugin\n";
  _pqp_analyzer.run();
  _dependency_validator.start();
}

void DependencyMiningPlugin::stop() {}

EXPORT_PLUGIN(DependencyMiningPlugin)

}  // namespace opossum
