#pragma once

#include "dependency_mining/pqp_analyzer.hpp"
#include "dependency_mining/util.hpp"
#include "dependency_mining/dependency_validator.hpp"
#include "utils/abstract_plugin.hpp"

namespace opossum {

class DependencyMiningPlugin : public AbstractPlugin {
 public:
  std::string description() const final;

  void start() final;

  void stop() final;

 protected:
  PQPAnalyzer _pqp_analyzer{};
  DependencyValidator _dependency_validator{};
  DependencyCandidateQueue _queue;
};

}  // namespace opossum
