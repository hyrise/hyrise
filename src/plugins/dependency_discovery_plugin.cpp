#include "dependency_discovery_plugin.hpp"

#include <boost/container_hash/hash.hpp>
#include "magic_enum.hpp"

#include "../benchmarklib/abstract_benchmark_item_runner.hpp"
#include "dependency_discovery/candidate_strategy/dependent_group_by_reduction_candidate_rule.hpp"
#include "dependency_discovery/candidate_strategy/join_to_predicate_candidate_rule.hpp"
#include "dependency_discovery/candidate_strategy/join_to_semi_join_candidate_rule.hpp"
#include "dependency_discovery/validation_strategy/fd_validation_rule.hpp"
#include "dependency_discovery/validation_strategy/ind_validation_rule.hpp"
#include "dependency_discovery/validation_strategy/od_validation_rule.hpp"
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
  _add_validation_rule(std::make_unique<OdValidationRule>());
  _add_validation_rule(std::make_unique<IndValidationRule>());
  _add_validation_rule(std::make_unique<FdValidationRule>());
}

std::string DependencyDiscoveryPlugin::description() const {
  return "Data Dependency Discovery Plugin";
}

void DependencyDiscoveryPlugin::start() {}

void DependencyDiscoveryPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
DependencyDiscoveryPlugin::provided_user_executable_functions() {
  return {{"DiscoverDependencies", [&]() { _discover_dependencies(); }}};
}

std::optional<PreBenchmarkHook> DependencyDiscoveryPlugin::pre_benchmark_hook() {
  return [&](auto& benchmark_item_runner) {
    for (const auto item_id : benchmark_item_runner.items()) {
      benchmark_item_runner.execute_item(item_id);
    }

    _discover_dependencies();

    for (const auto& log_entry : Hyrise::get().log_manager.log_entries()) {
      std::cout << log_entry.message << std::endl;
    }
  };
}

void DependencyDiscoveryPlugin::_discover_dependencies() const {
  auto discovery_timer = Timer{};

  _validate_dependency_candidates(_identify_dependency_candidates());

  auto message = std::stringstream{};
  message << "Executed dependency discovery in " << discovery_timer.lap_formatted();
  Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
}

DependencyCandidates DependencyDiscoveryPlugin::_identify_dependency_candidates() const {
  auto generation_timer = Timer{};
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

  auto message = std::stringstream{};
  message << "Generated " << dependency_candidates.size() << " candidates in " << generation_timer.lap_formatted();
  Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
  return dependency_candidates;
}

void DependencyDiscoveryPlugin::_validate_dependency_candidates(
    const DependencyCandidates& dependency_candidates) const {
  auto validation_timer = Timer{};
  for (const auto& candidate : dependency_candidates) {
    auto message = std::stringstream{};
    DebugAssert(_validation_rules.contains(candidate->type),
                "Unsupported dependency: " + std::string{magic_enum::enum_name(candidate->type)});

    auto candidate_timer = Timer{};
    const auto& validation_rule = _validation_rules.at(candidate->type);
    message << "Checking " << *candidate;
    const auto& result = validation_rule->validate(*candidate);

    switch (result.status) {
      case ValidationStatus::Invalid:
        message << " [rejected in " << candidate_timer.lap_formatted() << "]";
        break;
      case ValidationStatus::AlreadyKnown:
        message << " [skipped (already known) in " << candidate_timer.lap_formatted() << "]";
        break;
      case ValidationStatus::Valid:
        message << " [confirmed in " << candidate_timer.lap_formatted() << "]";
        Assert(!result.constraints.empty(),
               "Expected validation to yield constraint(s) for " + candidate->description());
        break;
      case ValidationStatus::Uncertain:
        Fail("Expected explicit validation result for " + candidate->description());
    }

    for (const auto& [table, constraint] : result.constraints) {
      _add_constraint(table, constraint);
    }
    Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
  }

  Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin",
                                        "Validated " + std::to_string(dependency_candidates.size()) +
                                            " candidates in " + validation_timer.lap_formatted(),
                                        LogLevel::Info);

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

void DependencyDiscoveryPlugin::_add_constraint(const std::shared_ptr<Table>& table,
                                                const std::shared_ptr<AbstractTableConstraint>& constraint) const {
  if (const auto& order_constraint = std::dynamic_pointer_cast<TableOrderConstraint>(constraint)) {
    table->add_soft_order_constraint(*order_constraint);
    return;
  }
  if (const auto& key_constraint = std::dynamic_pointer_cast<TableKeyConstraint>(constraint)) {
    table->add_soft_key_constraint(*key_constraint);
    return;
  }
  if (const auto& foreign_key_constraint = std::dynamic_pointer_cast<ForeignKeyConstraint>(constraint)) {
    table->add_soft_foreign_key_constraint(*foreign_key_constraint);
    return;
  }

  Fail("Invalid table constraint.");
}

EXPORT_PLUGIN(DependencyDiscoveryPlugin);

}  // namespace hyrise
