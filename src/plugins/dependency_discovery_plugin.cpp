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

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

void add_constraint(const std::shared_ptr<Table>& table, const std::shared_ptr<AbstractTableConstraint>& constraint) {
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

}  // namespace

namespace hyrise {

DependencyDiscoveryPlugin::DependencyDiscoveryPlugin() {
  const auto allow_dependent_groupby = std::getenv("DEPENDENT_GROUPBY");
  if (!allow_dependent_groupby || !std::strcmp(allow_dependent_groupby, "1")) {
    std::cout << "- Enable Dependent Group-by Reduction" << std::endl;
    _add_candidate_rule(std::make_unique<DependentGroupByReductionCandidateRule>());
  }

  const auto allow_join_to_semi = std::getenv("JOIN_TO_SEMI");
  if (!allow_join_to_semi || !std::strcmp(allow_join_to_semi, "1")) {
    std::cout << "- Enable Join to Semi-join" << std::endl;
    _add_candidate_rule(std::make_unique<JoinToSemiJoinCandidateRule>());
  }

  const auto allow_join_to_predicate = std::getenv("JOIN_TO_PREDICATE");
  if (!allow_join_to_predicate || !std::strcmp(allow_join_to_predicate, "1")) {
    std::cout << "- Enable Join to Predicate" << std::endl;
    _add_candidate_rule(std::make_unique<JoinToPredicateCandidateRule>());
  }

  _add_validation_rule(std::make_unique<UccValidationRule>());
  _add_validation_rule(std::make_unique<OdValidationRule>());
  _add_validation_rule(std::make_unique<IndValidationRule>());
  _add_validation_rule(std::make_unique<FdValidationRule>());

  const auto loop_count = std::getenv("VALIDATION_LOOPS");
  if (loop_count) {
    const auto requested_repetitions = std::atol(loop_count);
    Assert(requested_repetitions > 0, "Validation must be executed at least once!");
    _validation_repetitions = static_cast<uint32_t>(requested_repetitions);
    if (_validation_repetitions > 1) {
      const auto allow_constraints = std::getenv("SCHEMA_CONSTRAINTS");
      Assert(allow_constraints && !std::strcmp(allow_constraints, "0"),
             "Looping validation only permitted if no schema constraints are added as it resets all constrints.");
    }
  }
  std::cout << "- Execute " << _validation_repetitions << " validation run(s)" << std::endl;
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
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();
  Hyrise::get().log_manager.add_message(
      "DependencyDiscoveryPlugin", "Cleared LQP and PQP caches in " + discovery_timer.lap_formatted(), LogLevel::Info);
}

DependencyCandidates DependencyDiscoveryPlugin::_identify_dependency_candidates() const {
  const auto lqp_cache = Hyrise::get().default_lqp_cache;
  if (!lqp_cache) {
    return {};
  }

  auto dependency_candidates = DependencyCandidates{};

  auto loop_times = std::chrono::nanoseconds{};
  for (auto repetition = uint32_t{0}; repetition < _validation_repetitions; ++repetition) {
    if (_validation_repetitions > 1) {
      dependency_candidates.clear();
    }

    auto loop_timer = Timer{};
    // Get a snapshot of the current LQP cache to work on all currently cached queries.
    const auto& snapshot = Hyrise::get().default_lqp_cache->snapshot();

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

    loop_times += loop_timer.lap();
  }

  const auto generation_time = loop_times / _validation_repetitions;
  auto message = std::stringstream{};
  message << "Generated " << dependency_candidates.size() << " candidates in " << format_duration(generation_time);
  Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
  return dependency_candidates;
}

void DependencyDiscoveryPlugin::_validate_dependency_candidates(
    const DependencyCandidates& dependency_candidates) const {
  const auto candidate_count = dependency_candidates.size();
  auto candidate_times = std::vector<std::chrono::nanoseconds>(candidate_count);
  auto ordered_candidates = std::vector<std::shared_ptr<AbstractDependencyCandidate>>{};

  auto valid_count = uint32_t{0};
  auto invalid_count = uint32_t{0};
  auto skipped_count = uint32_t{0};

  auto loop_times = std::chrono::nanoseconds{};
  for (auto repetition = uint32_t{0}; repetition < _validation_repetitions; ++repetition) {
    // Reset all validation results and constraints.
    if (_validation_repetitions > 1) {
      valid_count = uint32_t{0};
      invalid_count = uint32_t{0};
      skipped_count = uint32_t{0};

      for (const auto& candidate : dependency_candidates) {
        candidate->status = ValidationStatus::Uncertain;
      }

      for (const auto& [_, table] : Hyrise::get().storage_manager.tables()) {
        table->_table_key_constraints.clear();
        table->_table_order_constraints.clear();
        table->_foreign_key_constraints.clear();
        table->_referenced_foreign_key_constraints.clear();
      }
    }

    auto loop_timer = Timer{};
    ordered_candidates = std::vector<std::shared_ptr<AbstractDependencyCandidate>>{dependency_candidates.cbegin(),
                                                                                   dependency_candidates.cend()};

    std::sort(ordered_candidates.begin(), ordered_candidates.end(),
              [](const auto& lhs, const auto& rhs) { return lhs->type < rhs->type; });

    for (auto candidate_id = size_t{0}; candidate_id < candidate_count; ++candidate_id) {
      const auto& candidate = ordered_candidates[candidate_id];
      DebugAssert(_validation_rules.contains(candidate->type),
                  "Unsupported dependency: " + std::string{magic_enum::enum_name(candidate->type)});

      auto candidate_timer = Timer{};
      const auto& validation_rule = _validation_rules.at(candidate->type);
      const auto& result = validation_rule->validate(*candidate);

      switch (result.status) {
        case ValidationStatus::Invalid:
          ++invalid_count;
          break;
        case ValidationStatus::AlreadyKnown:
          ++valid_count;
          break;
        case ValidationStatus::Valid:
          Assert(!result.constraints.empty(),
                 "Expected validation to yield constraint(s) for " + candidate->description());
          ++valid_count;
          break;
        case ValidationStatus::Superfluous:
          ++skipped_count;
          break;
        case ValidationStatus::Uncertain:
          Fail("Expected explicit validation result for " + candidate->description());
      }
      candidate_times[candidate_id] += candidate_timer.lap();
      candidate->status = result.status;

      for (const auto& [table, constraint] : result.constraints) {
        add_constraint(table, constraint);
      }
    }
    loop_times += loop_timer.lap();
  }

  for (auto candidate_id = size_t{0}; candidate_id < candidate_count; ++candidate_id) {
    const auto& candidate = ordered_candidates[candidate_id];
    auto message = std::stringstream{};
    DebugAssert(_validation_rules.contains(candidate->type),
                "Unsupported dependency: " + std::string{magic_enum::enum_name(candidate->type)});

    message << "Checking " << *candidate;
    const auto mean_candidate_time = candidate_times[candidate_id] / _validation_repetitions;

    switch (candidate->status) {
      case ValidationStatus::Invalid:
        message << " [rejected in " << format_duration(mean_candidate_time) << "]";
        break;
      case ValidationStatus::AlreadyKnown:
        message << " [skipped (already known) in " << format_duration(mean_candidate_time) << "]";
        break;
      case ValidationStatus::Valid:
        message << " [confirmed in " << format_duration(mean_candidate_time) << "]";
        break;
      case ValidationStatus::Superfluous:
        message << " [skipped (not required anymore) in " << format_duration(mean_candidate_time) << "]";
        break;
      case ValidationStatus::Uncertain:
        Fail("Expected explicit validation result for " + candidate->description());
    }
    Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
  }

  Assert(valid_count + invalid_count + skipped_count == dependency_candidates.size(),
         "Numbers of candidates do not add up.");
  auto message = std::stringstream{};
  const auto validation_time = loop_times / _validation_repetitions;
  message << "Validated " << dependency_candidates.size() << " candidates (" << valid_count << " valid, "
          << invalid_count << " invalid, " << skipped_count << " superfluous) in " << format_duration(validation_time);
  Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
}

void DependencyDiscoveryPlugin::_add_candidate_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule) {
  _candidate_rules[rule->target_node_type].emplace_back(std::move(rule));
}

void DependencyDiscoveryPlugin::_add_validation_rule(std::unique_ptr<AbstractDependencyValidationRule> rule) {
  _validation_rules[rule->dependency_type] = std::move(rule);
}

EXPORT_PLUGIN(DependencyDiscoveryPlugin);

}  // namespace hyrise
