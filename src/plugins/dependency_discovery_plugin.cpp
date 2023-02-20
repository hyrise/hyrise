#include "dependency_discovery_plugin.hpp"

#include <boost/container_hash/hash.hpp>
#include "magic_enum.hpp"

#include "../benchmarklib/abstract_benchmark_item_runner.hpp"
#include "dependency_discovery/candidate_strategy/dependent_group_by_reduction_candidate_rule.hpp"
#include "dependency_discovery/candidate_strategy/join_to_predicate_candidate_rule.hpp"
#include "dependency_discovery/candidate_strategy/join_to_semi_join_candidate_rule.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "resolve_type.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace hyrise {

DependencyDiscoveryPlugin::DependencyDiscoveryPlugin() {
  add_rule(std::make_unique<DependentGroupByReductionCandidateRule>());
  add_rule(std::make_unique<JoinToSemiJoinCandidateRule>());
  add_rule(std::make_unique<JoinToPredicateCandidateRule>());
}

std::string DependencyDiscoveryPlugin::description() const {
  return "Unary Unique Column Combination Discovery Plugin";
}

void DependencyDiscoveryPlugin::start() {}

void DependencyDiscoveryPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
DependencyDiscoveryPlugin::provided_user_executable_functions() {
  return {{"DiscoverUCCs", [&]() { _validate_ucc_candidates(_identify_ucc_candidates()); }}};
}

std::optional<PreBenchmarkHook> DependencyDiscoveryPlugin::pre_benchmark_hook() {
  return [&](auto& benchmark_item_runner) {
    for (const auto item_id : benchmark_item_runner.items()) {
      benchmark_item_runner.execute_item(item_id);
    }
    _validate_ucc_candidates(_identify_ucc_candidates());
  };
}

DependencyCandidates DependencyDiscoveryPlugin::_identify_ucc_candidates() {
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

      for (const auto& rule : _rules[type]) {
        rule->apply_to_node(node, dependency_candidates);
      }

      return LQPVisitation::VisitInputs;
    });
  }

  return dependency_candidates;
}

void DependencyDiscoveryPlugin::_validate_ucc_candidates(const DependencyCandidates& ucc_candidates) {
  for (const auto& candidate : ucc_candidates) {
    auto message = std::stringstream{};
    if (candidate->type != DependencyType::UniqueColumn) {
      message << "Skipping candidate " << *candidate << " (not implemented)" << std::endl;
      Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
      continue;
    }

    auto candidate_timer = Timer();
    const auto table = Hyrise::get().storage_manager.get_table(candidate->table_name);
    const auto column_id = candidate->column_id;
    message << "Checking candidate " << *candidate << std::endl;

    const auto& soft_key_constraints = table->soft_key_constraints();

    // Skip already discovered UCCs.
    if (std::any_of(soft_key_constraints.cbegin(), soft_key_constraints.cend(),
                    [&column_id](const auto& key_constraint) {
                      const auto& columns = key_constraint.columns();
                      return columns.size() == 1 && columns.front() == column_id;
                    })) {
      message << " [skipped (already known) in " << candidate_timer.lap_formatted() << "]";
      Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
      continue;
    }

    resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      // Utilize efficient check for uniqueness inside each dictionary segment for a potential early out.
      if (_dictionary_segments_contain_duplicates<ColumnDataType>(table, column_id)) {
        message << " [rejected in " << candidate_timer.lap_formatted() << "]";
        Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
        return;
      }

      // If we reach here, we have to run the more expensive cross-segment duplicate check.
      if (!_uniqueness_holds_across_segments<ColumnDataType>(table, column_id)) {
        message << " [rejected in " << candidate_timer.lap_formatted() << "]";
        Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
        return;
      }

      // We save UCCs directly inside the table so they can be forwarded to nodes in a query plan.
      message << " [confirmed in " << candidate_timer.lap_formatted() << "]";
      Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", message.str(), LogLevel::Info);
      table->add_soft_key_constraint(TableKeyConstraint({column_id}, KeyConstraintType::UNIQUE));
    });
  }
  Hyrise::get().log_manager.add_message("DependencyDiscoveryPlugin", "Clearing LQP and PQP cache...", LogLevel::Debug);

  Hyrise::get().default_lqp_cache->clear();
  Hyrise::get().default_pqp_cache->clear();
}

template <typename ColumnDataType>
bool DependencyDiscoveryPlugin::_dictionary_segments_contain_duplicates(std::shared_ptr<Table> table,
                                                                        ColumnID column_id) {
  const auto chunk_count = table->chunk_count();
  // Trigger an early-out if a dictionary-encoded segment's attribute vector is larger than the dictionary. This indica-
  // tes that at least one duplicate value or a NULL value is contained.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    if (!source_chunk) {
      continue;
    }
    const auto source_segment = source_chunk->get_segment(column_id);
    if (!source_segment) {
      continue;
    }

    if (const auto& dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment)) {
      if (dictionary_segment->unique_values_count() != dictionary_segment->size()) {
        return true;
      }
    } else if (const auto& fixed_string_dictionary_segment =
                   std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(source_segment)) {
      if (fixed_string_dictionary_segment->unique_values_count() != fixed_string_dictionary_segment->size()) {
        return true;
      }
    } else {
      // If any segment is not dictionary-encoded, we have to perform a full validation.
      return false;
    }
  }
  return false;
}

template <typename ColumnDataType>
bool DependencyDiscoveryPlugin::_uniqueness_holds_across_segments(std::shared_ptr<Table> table, ColumnID column_id) {
  const auto chunk_count = table->chunk_count();
  // `distinct_values` collects the segment values from all chunks.
  auto distinct_values = std::unordered_set<ColumnDataType>{};

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    if (!source_chunk) {
      continue;
    }
    const auto source_segment = source_chunk->get_segment(column_id);
    if (!source_segment) {
      continue;
    }

    const auto expected_distinct_value_count = distinct_values.size() + source_segment->size();

    if (const auto& value_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(source_segment)) {
      // Directly insert all values.
      const auto& values = value_segment->values();
      distinct_values.insert(values.cbegin(), values.cend());
    } else if (const auto& dictionary_segment =
                   std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment)) {
      // Directly insert dictionary entries.
      const auto& dictionary = dictionary_segment->dictionary();
      distinct_values.insert(dictionary->cbegin(), dictionary->cend());
    } else {
      // Fallback: Iterate the whole segment and decode its values.
      auto distinct_value_count = distinct_values.size();
      segment_with_iterators<ColumnDataType>(*source_segment, [&](auto it, const auto end) {
        while (it != end) {
          if (it->is_null()) {
            break;
          }
          distinct_values.insert(it->value());
          if (distinct_value_count + 1 != distinct_values.size()) {
            break;
          }
          ++distinct_value_count;
          ++it;
        }
      });
    }

    // If not all elements have been inserted, there must be a duplicate, so the UCC is violated.
    if (distinct_values.size() != expected_distinct_value_count) {
      return false;
    }
  }

  return true;
}

void DependencyDiscoveryPlugin::add_rule(std::unique_ptr<AbstractDependencyCandidateRule> rule) {
  _rules[rule->target_node_type].emplace_back(std::move(rule));
}

EXPORT_PLUGIN(DependencyDiscoveryPlugin);

}  // namespace hyrise
