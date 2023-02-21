#include "dependency_discovery_plugin.hpp"

#include <boost/container_hash/hash.hpp>
#include "magic_enum.hpp"

#include "../benchmarklib/abstract_benchmark_item_runner.hpp"
#include "dependency_discovery/candidate_strategy/dependent_group_by_reduction_candidate_rule.hpp"
#include "dependency_discovery/candidate_strategy/join_to_predicate_candidate_rule.hpp"
#include "dependency_discovery/candidate_strategy/join_to_semi_join_candidate_rule.hpp"
#include "dependency_discovery/validation_strategy/ucc_validation_rule.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace hyrise {

DependencyDiscoveryPlugin::DependencyDiscoveryPlugin() {
  _add_candidate_rule(std::make_unique<DependentGroupByReductionCandidateRule>());
  _add_candidate_rule(std::make_unique<JoinToSemiJoinCandidateRule>());
  _add_candidate_rule(std::make_unique<JoinToPredicateCandidateRule>());

  _add_validation_rule(std::make_unique<UccValidationRule>());
}

std::string DependencyDiscoveryPlugin::description() const {
  return "Unary Unique Column Combination Discovery Plugin";
}

void DependencyDiscoveryPlugin::start() {}

void DependencyDiscoveryPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
DependencyDiscoveryPlugin::provided_user_executable_functions() {
  return {{"DiscoverUCCs", [&]() { _validate_dependency_candidates(_identify_dependency_candidates()); }}};
}

std::optional<PreBenchmarkHook> DependencyDiscoveryPlugin::pre_benchmark_hook() {
  return [&](auto& benchmark_item_runner) {
    for (const auto item_id : benchmark_item_runner.items()) {
      benchmark_item_runner.execute_item(item_id);
    }
    _validate_dependency_candidates(_identify_dependency_candidates());
  };
}

DependencyCandidates DependencyDiscoveryPlugin::_identify_dependency_candidates() const {
  const auto lqp_cache = Hyrise::get().default_lqp_cache;
  if (!lqp_cache) {
    return {};
  }

  // Get a snapshot of the current LQP cache to work on all currently cached queries.
  const auto& snapshot = Hyrise::get().default_lqp_cache->snapshot();
  auto dependency_candidates = DependencyCandidates{};

  for (const auto& [_, entry] : snapshot) {
    const auto& root_node = entry.value;

    visit_lqp(root_node, [&](const auto& node) {
      const auto type = node->type;

      const auto& rule_it = _candidate_rules.find(type);
      if (rule_it == _candidate_rules.end()) {
        return LQPVisitation::VisitInputs;
      }

      for (const auto& candidate_rule : rule_it->second) {
        candidate_rule->apply_to_node(node, dependency_candidates);
      }

      return LQPVisitation::VisitInputs;
    });
  }

  return dependency_candidates;
}

void DependencyDiscoveryPlugin::_validate_dependency_candidates(
    const DependencyCandidates& dependency_candidates) const {
  for (const auto& candidate : dependency_candidates) {
    auto message = std::stringstream{};

    if (!_validation_rules.contains(candidate->type)) {
      message << "Skipping candidate " << *candidate << " (not implemented)" << std::endl;
      Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
      continue;
    }

    auto candidate_timer = Timer{};
    const auto& validation_rule = _validation_rules.at(candidate->type);
    message << "Checking candidate " << *candidate << std::endl;

    const auto& result = validation_rule->validate(*candidate);

    if (result.status == ValidationStatus::Invalid) {
      message << " [rejected in " << candidate_timer.lap_formatted() << "]";
      Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
      continue;
    }

    message << " [confirmed in " << candidate_timer.lap_formatted() << "]";
    Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
  }

  Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", "Clearing LQP and PQP cache...", LogLevel::Debug);
  Hyrise::get().default_lqp_cache->clear();
  Hyrise::get().default_pqp_cache->clear();
}

void DependencyDiscoveryPlugin::_add_candidate_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule) {
  _candidate_rules[rule->target_node_type].emplace_back(std::move(rule));
}

void DependencyDiscoveryPlugin::_add_validation_rule(std::unique_ptr<AbstractDependencyValidationRule> rule) {
  _validation_rules[rule->dependency_type] = std::move(rule);
}

EXPORT_PLUGIN(DependencyDiscoveryPlugin);

}  // namespace hyrise
