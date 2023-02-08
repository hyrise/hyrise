#include "ucc_discovery_plugin.hpp"

#include <boost/container_hash/hash.hpp>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/value_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "magic_enum.hpp"
#include "resolve_type.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

namespace hyrise {

UccCandidate::UccCandidate(const std::string& init_table_name, const ColumnID init_column_id)
    : table_name(init_table_name), column_id(init_column_id) {}

bool UccCandidate::operator==(const UccCandidate& other) const {
  return (column_id == other.column_id) && (table_name == other.table_name);
}

bool UccCandidate::operator!=(const UccCandidate& other) const {
  return !(other == *this);
}

size_t UccCandidate::hash() const {
  auto hash = boost::hash_value(table_name);
  boost::hash_combine(hash, column_id);
  return hash;
}

std::string UccDiscoveryPlugin::description() const {
  return "Unary Unique Column Combination Discovery Plugin";
}

void UccDiscoveryPlugin::start() {}

void UccDiscoveryPlugin::stop() {}

std::vector<std::pair<PluginFunctionName, PluginFunctionPointer>>
UccDiscoveryPlugin::provided_user_executable_functions() {
  return {{"DiscoverUCCs", [&]() { _validate_ucc_candidates(_identify_ucc_candidates()); }}};
}

UccCandidates UccDiscoveryPlugin::_identify_ucc_candidates() {
  const auto lqp_cache = Hyrise::get().default_lqp_cache;
  if (!lqp_cache) {
    return {};
  }

  // Get a snapshot of the current LQP cache to work on all currently cached queries.
  const auto& snapshot = Hyrise::get().default_lqp_cache->snapshot();

  auto ucc_candidates = UccCandidates{};

  for (const auto& [_, entry] : snapshot) {
    const auto& root_node = entry.value;

    visit_lqp(root_node, [&](const auto& node) {
      const auto type = node->type;
      if (type != LQPNodeType::Join && type != LQPNodeType::Aggregate) {
        // Only join and aggregate (Groupby) nodes are considered for optimization using UCCs.
        return LQPVisitation::VisitInputs;
      }

      if (type == LQPNodeType::Aggregate) {
        _ucc_candidates_from_aggregate_node(node, ucc_candidates);
        return LQPVisitation::VisitInputs;
      }
      // The only remaining option is a join node.
      _ucc_candidates_from_join_node(node, ucc_candidates);

      return LQPVisitation::VisitInputs;
    });
  }

  return ucc_candidates;
}

void UccDiscoveryPlugin::_validate_ucc_candidates(const UccCandidates& ucc_candidates) {
  for (const auto& candidate : ucc_candidates) {
    auto candidate_timer = Timer();
    const auto table = Hyrise::get().storage_manager.get_table(candidate.table_name);
    const auto column_id = candidate.column_id;

    auto message = std::stringstream{};
    message << "Checking candidate " << candidate.table_name << "." << table->column_name(column_id);

    const auto& soft_key_constraints = table->soft_key_constraints();

    // Skip already discovered UCCs.
    if (std::any_of(soft_key_constraints.cbegin(), soft_key_constraints.cend(),
                    [&column_id](const auto& key_constraint) {
                      const auto& columns = key_constraint.columns();
                      return columns.size() == 1 && columns.front() == column_id;
                    })) {
      message << " [skipped (already known) in " << candidate_timer.lap_formatted() << "]";
      Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
      continue;
    }

    resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      // Utilize efficient check for uniqueness inside each dictionary segment for a potential early out.
      if (_dictionary_segments_contain_duplicates<ColumnDataType>(table, column_id)) {
        message << " [rejected in " << candidate_timer.lap_formatted() << "]";
        Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
        return;
      }

      // If we reach here, we have to run the more expensive cross-segment duplicate check.
      if (!_uniqueness_holds_across_segments<ColumnDataType>(table, column_id)) {
        message << " [rejected in " << candidate_timer.lap_formatted() << "]";
        Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
        return;
      }

      // We save UCCs directly inside the table so they can be forwarded to nodes in a query plan.
      message << " [confirmed in " << candidate_timer.lap_formatted() << "]";
      Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
      table->add_soft_key_constraint(TableKeyConstraint({column_id}, KeyConstraintType::UNIQUE));
    });
  }
  Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", "Clearing LQP and PQP cache...", LogLevel::Debug);

  Hyrise::get().default_lqp_cache->clear();
  Hyrise::get().default_pqp_cache->clear();
}

template <typename ColumnDataType>
bool UccDiscoveryPlugin::_dictionary_segments_contain_duplicates(std::shared_ptr<Table> table, ColumnID column_id) {
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
bool UccDiscoveryPlugin::_uniqueness_holds_across_segments(std::shared_ptr<Table> table, ColumnID column_id) {
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

void UccDiscoveryPlugin::_ucc_candidates_from_aggregate_node(std::shared_ptr<AbstractLQPNode> node,
                                                             UccCandidates& ucc_candidates) {
  const auto& aggregate_node = static_cast<AggregateNode&>(*node);
  const auto column_candidates = std::vector<std::shared_ptr<AbstractExpression>>{
      aggregate_node.node_expressions.cbegin(),
      aggregate_node.node_expressions.cbegin() + aggregate_node.aggregate_expressions_begin_idx};

  for (const auto& column_candidate : column_candidates) {
    const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(column_candidate);
    if (!lqp_column_expression) {
      continue;
    }
    // Every ColumnExpression used as a GroupBy expression should be checked for uniqueness.
    const auto& stored_table_node = static_cast<const StoredTableNode&>(*lqp_column_expression->original_node.lock());
    ucc_candidates.insert(UccCandidate{stored_table_node.table_name, lqp_column_expression->original_column_id});
  }
}

void UccDiscoveryPlugin::_ucc_candidates_from_join_node(std::shared_ptr<AbstractLQPNode> node,
                                                        UccCandidates& ucc_candidates) {
  const auto& join_node = static_cast<JoinNode&>(*node);
  // Fetch the join predicate to extract the UCC candidates from. Right now, limited to single-predicate equi joins.
  const auto& join_predicates = join_node.join_predicates();
  if (join_predicates.size() != 1) {
    return;
  }
  const auto binary_join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates.front());
  if (!binary_join_predicate || binary_join_predicate->predicate_condition != PredicateCondition::Equals) {
    return;
  }

  const auto column_expression_for_join_input = [&](const auto& input_node) {
    // The join predicate may be swapped, so get the proper operand.
    auto column_candidate = std::dynamic_pointer_cast<LQPColumnExpression>(binary_join_predicate->left_operand());
    if (!column_candidate || !expression_evaluable_on_lqp(column_candidate, *input_node)) {
      column_candidate = std::dynamic_pointer_cast<LQPColumnExpression>(binary_join_predicate->right_operand());
    }
    // We do not have to find an LQPColumnExpression in the join predicates at all, e.g., when joining on substrings:
    // SELECT * FROM orders, lineitem WHERE EXTRACT(YEAR FROM o_orderdate) = EXTRACT(YEAR FROM l_shipdate).
    // Thus, column_candidate might be a nullptr.
    Assert(!column_candidate || expression_evaluable_on_lqp(column_candidate, *input_node),
           "Join predicate should belong to an input");
    return column_candidate;
  };

  // We only care about semi (right input is candidate) and inner (both are potential candidates) joins.
  switch (join_node.join_mode) {
    case JoinMode::Inner: {
      for (const auto& column_candidate : binary_join_predicate->arguments) {
        const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(column_candidate);
        if (!lqp_column_expression) {
          continue;
        }

        // Determine which subtree (left or right) belongs to the ColumnExpression.
        auto subtree_root = join_node.left_input();
        if (!expression_evaluable_on_lqp(column_candidate, *subtree_root)) {
          subtree_root = join_node.right_input();
        }

        // For JoinToSemiJoinRule, the join column is already interesting for optimization.
        const auto& original_node = lqp_column_expression->original_node.lock();
        if (original_node->type != LQPNodeType::StoredTable) {
          continue;
        }
        const auto& stored_table_node = static_cast<const StoredTableNode&>(*original_node);
        ucc_candidates.insert(UccCandidate{stored_table_node.table_name, lqp_column_expression->original_column_id});

        _ucc_candidates_from_removable_join_input(subtree_root, lqp_column_expression, ucc_candidates);
      }
      return;
    }

    case JoinMode::Semi: {
      // We want to check only the right hand side here, as this is the one that will be removed in the end.
      const auto subtree_root = join_node.right_input();
      const auto column_candidate = column_expression_for_join_input(subtree_root);
      _ucc_candidates_from_removable_join_input(subtree_root, column_candidate, ucc_candidates);
      return;
    }

    case JoinMode::Left:
    case JoinMode::Right:
    case JoinMode::FullOuter:
    case JoinMode::Cross:
    case JoinMode::AntiNullAsTrue:
    case JoinMode::AntiNullAsFalse:
      return;
  }
}

void UccDiscoveryPlugin::_ucc_candidates_from_removable_join_input(
    std::shared_ptr<AbstractLQPNode> root_node, std::shared_ptr<LQPColumnExpression> column_candidate,
    UccCandidates& ucc_candidates) {
  // The input node may already be a nullptr in case we try to get the right input of node with only one input. The can-
  // didate Column might be a nullptr when the join is not performed on bare columns but, e.g., on aggregates.
  if (!root_node || !column_candidate) {
    return;
  }

  visit_lqp(root_node, [&](auto& node) {
    if (node->type != LQPNodeType::Predicate) {
      return LQPVisitation::VisitInputs;
    }

    // When we find a predicate node, we check whether the searched column is filtered in this predicate. If so, it is a
    // valid UCC candidate; if not, continue searching.
    const auto& predicate_node = static_cast<const PredicateNode&>(*node);

    // Ensure that we look at a binary predicate expression checking for equality (e.g., a = 'x').
    const auto predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node.predicate());
    if (!predicate || predicate->predicate_condition != PredicateCondition::Equals) {
      return LQPVisitation::VisitInputs;
    }

    // Get the column expression, which is not always the left operand.
    auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->left_operand());
    auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->right_operand());
    if (!column_expression) {
      column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->right_operand());
      value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->left_operand());
    }

    if (!column_expression || !value_expression) {
      // The predicate needs to look like column = value or value = column; if not, move on.
      return LQPVisitation::VisitInputs;
    }

    const auto expression_table_node =
        std::static_pointer_cast<const StoredTableNode>(column_expression->original_node.lock());
    const auto candidate_table_node =
        std::static_pointer_cast<const StoredTableNode>(column_candidate->original_node.lock());

    if (expression_table_node == candidate_table_node) {
      // Both columns should be in the same table.
      ucc_candidates.emplace(expression_table_node->table_name, column_expression->original_column_id);
      ucc_candidates.emplace(candidate_table_node->table_name, column_candidate->original_column_id);
    }

    return LQPVisitation::VisitInputs;
  });
}

EXPORT_PLUGIN(UccDiscoveryPlugin)

}  // namespace hyrise

namespace std {

size_t hash<hyrise::UccCandidate>::operator()(const hyrise::UccCandidate& ucc_candidate) const {
  return ucc_candidate.hash();
}

}  // namespace std
