#include "dependency_mining_plugin.hpp"

#include "hyrise.hpp"

namespace opossum {

std::string DependencyMiningPlugin::description() const { return "Dependency Mining Plugin"; }

DependencyMiningPlugin::DependencyMiningPlugin()
    : _queue(std::make_shared<DependencyCandidateQueue>()), _pqp_analyzer(_queue) {
  //_queue = std::make_shared<DependencyCandidateQueue>();
  //_pqp_analyzer(_queue);
  //_dependency_validator(_queue);
  for (size_t validator_id{0}; validator_id < NUM_VALIDATORS; ++validator_id) {
    _dependency_validators.emplace_back(std::make_unique<DependencyValidator>(_queue));
  }
}

void DependencyMiningPlugin::start() {
  //_queue = std::make_shared<tbb::concurrent_priority_queue<DependencyCandidate>>();
  //_pqp_analyzer.set_queue(_queue);
  //_dependency_validator.set_queue(_queue);
  std::cout << "====================================================\nStarting Dependency Mining Plugin\n";
  _pqp_analyzer.run();
  for (auto& validator : _dependency_validators) {
    validator->start();
  }
}

void DependencyMiningPlugin::stop() {}

EXPORT_PLUGIN(DependencyMiningPlugin)

}  // namespace opossum
