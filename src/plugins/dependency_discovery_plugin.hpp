#pragma once

#include "dependency_discovery/candidate_strategy/abstract_dependency_candidate_rule.hpp"
#include "dependency_discovery/dependency_candidates.hpp"
#include "dependency_discovery/validation_strategy/abstract_dependency_validation_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "storage/table.hpp"
#include "types.hpp"
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
  DependencyCandidates _identify_dependency_candidates();

  /**
   * Iterates over the provided set of columns identified as candidates for a uniqueness validation. Validates those
   * that are not already known to be unique.
   */
  void _validate_dependency_candidates(const DependencyCandidates& dependency_candidates);

 private:
  /**
   * Checks whether individual DictionarySegments contain duplicates. This is an efficient operation as the check is
   * simply comparing the length of the dictionary to that of the attribute vector. This function can therefore be used
   * for an early-out before the more expensive cross-segment uniqueness check.
   */
  template <typename ColumnDataType>
  static bool _dictionary_segments_contain_duplicates(std::shared_ptr<Table> table, ColumnID column_id);

  /**
   * Checks whether the given table contains only unique values by inserting all values into an unordered set. If for
   * any table segment the size of the set increases by less than the number of values in that segment, we know that
   * there must be a duplicate and return false. Otherwise, returns true.
   */
  template <typename ColumnDataType>
  static bool _uniqueness_holds_across_segments(std::shared_ptr<Table> table, ColumnID column_id);

  void _add_candidate_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule);

  void _add_validation_rule(std::unique_ptr<AbstractDependencyValidationRule> rule);

  std::unordered_map<LQPNodeType, std::vector<std::unique_ptr<AbstractDependencyCandidateRule>>> _candidate_rules{};

  std::unordered_map<DependencyType, std::unique_ptr<AbstractDependencyValidationRule>> _validation_rules{};
};

}  // namespace hyrise
