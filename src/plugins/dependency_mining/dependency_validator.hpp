#pragma once

#include <tbb/concurrent_unordered_map.h>
#include <atomic>
#include <mutex>

#include "util.hpp"
#include "validation_strategy/abstract_dependency_validation_rule.hpp"

namespace opossum {

class DependencyValidator {
 public:
  DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue,
                      tbb::concurrent_unordered_map<std::string, std::shared_ptr<std::mutex>>& table_constraint_mutexes,
                      size_t id);

 protected:
  friend class DependencyMiningPlugin;
  void start();
  void stop();
  void add_rule(std::unique_ptr<AbstractDependencyValidationRule> rule);

 private:
  const std::shared_ptr<DependencyCandidateQueue>& _queue;
  std::unordered_map<DependencyType, std::unique_ptr<AbstractDependencyValidationRule>> _rules;
  std::atomic_bool _running = false;
  const size_t _id;
};

}  // namespace opossum
