#pragma once

#include "dependency_mining/dependency_validator.hpp"
#include "dependency_mining/pqp_analyzer.hpp"
#include "dependency_mining/util.hpp"
#include "utils/abstract_plugin.hpp"

namespace opossum {

class DependencyMiningPlugin : public AbstractPlugin {
 public:
  explicit DependencyMiningPlugin();
  std::string description() const final;

  void start() final;

  void stop() final;

  constexpr static size_t NUM_VALIDATORS = 3;

 protected:
  std::shared_ptr<DependencyCandidateQueue> _queue;
  PQPAnalyzer _pqp_analyzer;
  std::vector<std::unique_ptr<DependencyValidator>> _dependency_validators;
};

}  // namespace opossum
