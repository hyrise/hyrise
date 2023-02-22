#pragma once

#include "dependency_discovery/candidate_strategy/abstract_dependency_candidate_rule.hpp"
#include "dependency_discovery/dependency_candidates.hpp"
#include "dependency_discovery/validation_strategy/abstract_dependency_validation_rule.hpp"
#include "utils/abstract_plugin.hpp"

namespace hyrise {

/**
 *  This plugin implements unary Unique Column Combination (UCC) discovery based on previously executed LQPs. Not all
 *  columns encountered in these LQPs are automatically considered for the UCC validation process. Instead, a column is
 *  only validated/invalidated as UCCs if being a UCC could have helped to optimize their LQP.
 */
class DependencyDiscoveryPlugin : public AbstractPlugin {
 public:
  DependencyDiscoveryPlugin();

  std::string description() const final;

  void start() final;

  void stop() final;

  std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>> provided_user_executable_functions() final;

  std::optional<PreBenchmarkHook> pre_benchmark_hook() final;

 protected:
  friend class DependencyDiscoveryPluginTest;

  /**
   * Takes a snapshot of the current LQP Cache. Iterates through the LQPs and tries to extract sensible columns as can-
   * didates for UCC validation from each of them. A column is added as candidates if being a UCC has the potential to
   * help optimize their respective LQP.
   * 
   * Returns an unordered set of these candidates to be used in the UCC validation function.
   */
  DependencyCandidates _identify_dependency_candidates() const;

  /**
   * Iterates over the provided set of columns identified as candidates for a uniqueness validation. Validates those
   * that are not already known to be unique.
   */
  void _validate_dependency_candidates(const DependencyCandidates& dependency_candidates) const;

 private:
  void _add_candidate_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule);

  void _add_validation_rule(std::unique_ptr<AbstractDependencyValidationRule> rule);

  void _add_constraint(const std::string& table_name, const std::shared_ptr<AbstractTableConstraint>& constraint) const;

  std::unordered_map<LQPNodeType, std::vector<std::unique_ptr<AbstractDependencyCandidateRule>>> _candidate_rules{};

  std::unordered_map<DependencyType, std::unique_ptr<AbstractDependencyValidationRule>> _validation_rules{};
};

}  // namespace hyrise
