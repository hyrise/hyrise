#include "dependency_mining_plugin.hpp"

#include "hyrise.hpp"
#include "utils/timer.hpp"

namespace opossum {

std::string DependencyMiningPlugin::description() const { return "Dependency Mining Plugin"; }

DependencyMiningPlugin::DependencyMiningPlugin()
    : _queue(std::make_shared<DependencyCandidateQueue>()), _pqp_analyzer(_queue) {
  //_queue = std::make_shared<DependencyCandidateQueue>();
  //_pqp_analyzer(_queue);
  //_dependency_validator(_queue);
}

void DependencyMiningPlugin::start() {
  //_queue = std::make_shared<tbb::concurrent_priority_queue<DependencyCandidate>>();
  //_pqp_analyzer.set_queue(_queue);
  //_dependency_validator.set_queue(_queue);
  Timer timer;
  std::cout << "====================================================\nStarting DependencyMiningPlugin\n";
  _pqp_analyzer.run();
  std::vector<std::thread> validator_threads;
  auto table_constraint_mutexes = tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>{};
  for (size_t validator_id{0}; validator_id < NUM_VALIDATORS; ++validator_id) {
    //_dependency_validators.emplace_back(std::make_unique<DependencyValidator>(_queue));
    validator_threads.emplace_back([&](size_t i){
      const auto validator = std::make_unique<DependencyValidator>(_queue, table_constraint_mutexes, i);
      validator->start();
    }, validator_id);
  }
  /*for (auto& validator : _dependency_validators) {
    validator->start();
  }*/
  for (auto& thread : validator_threads) thread.join();
  std::cout << "Clear Cache" << std::endl;
  Hyrise::get().default_pqp_cache->clear();
  Hyrise::get().default_lqp_cache->clear();
  std::cout << "DependencyMiningPlugin finished in " << timer.lap_formatted() << std::endl;
}

void DependencyMiningPlugin::stop() {}

EXPORT_PLUGIN(DependencyMiningPlugin)

}  // namespace opossum
