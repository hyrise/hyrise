#pragma once

#include <atomic>
#include <mutex>

#include "util.hpp"
#include "validation_state.hpp"
#include "validation_strategy/abstract_dependency_validation_rule.hpp"

namespace opossum {

class DependencyValidator {
 public:
  DependencyValidator(const std::shared_ptr<DependencyCandidateQueue>& queue, size_t id,
                      const std::shared_ptr<ValidationState>& validation_state);

 protected:
  friend class DependencyMiningPlugin;
  void start();
  void stop();
  void add_rule(std::unique_ptr<AbstractDependencyValidationRule> rule);

 private:
  void _add_constraints(const std::string& table_name,
                        const std::vector<std::shared_ptr<AbstractTableConstraint>>& constraints) const;
  const std::shared_ptr<DependencyCandidateQueue> _queue;
  std::unordered_map<DependencyType, std::unique_ptr<AbstractDependencyValidationRule>> _rules;
  std::atomic_bool _running = false;
  const size_t _id;
  std::shared_ptr<ValidationState> _validation_state;
};

}  // namespace opossum
