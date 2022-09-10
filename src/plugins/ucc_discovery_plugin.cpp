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
#include "utils/format_duration.hpp"
#include "utils/timer.hpp"

#include "resolve_type.hpp"

namespace hyrise {

UCCCandidate::UCCCandidate(const std::string& table_name, const ColumnID column_id)
    : table_name(table_name), column_id(column_id) {}

bool UCCCandidate::operator==(const UCCCandidate& other) const {
  return (column_id == other.column_id) && (table_name == other.table_name);
}

bool UCCCandidate::operator!=(const UCCCandidate& other) const {
  return !(other == *this);
}

size_t UCCCandidate::hash() const {
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

UCCCandidates UccDiscoveryPlugin::_identify_ucc_candidates() const {
  // get a snapshot of the current LQP cache to work on all currently cached queries
  const auto snapshot = Hyrise::get().default_lqp_cache->snapshot();

  auto ucc_candidates = UCCCandidates{};

  for (const auto& [_, entry] : snapshot) {
    const auto& root_node = entry.value;

    visit_lqp(root_node, [&](auto& node) {
      const auto type = node->type;
      if (type != LQPNodeType::Join && type != LQPNodeType::Aggregate) {
        // Non-Join and Non-Aggregate (Groupby) nodes are not considered for optimization using UCCs
        return LQPVisitation::VisitInputs;
      }

      if (type == LQPNodeType::Aggregate) {
        _ucc_candidates_from_aggregate_node(node, ucc_candidates);
        return LQPVisitation::VisitInputs;
      }
      // no need for if statement; only remaining option is a join node
      _ucc_candidates_from_join_node(node, ucc_candidates);

      return LQPVisitation::VisitInputs;
    });
  }

  return ucc_candidates;
}

void UccDiscoveryPlugin::_validate_ucc_candidates(const UCCCandidates& ucc_candidates) const {
  for (const auto& candidate : ucc_candidates) {
    auto candidate_time = Timer();
    const auto table = Hyrise::get().storage_manager.get_table(candidate.table_name);
    const auto col_id = candidate.column_id;

    std::ostringstream message;

    message << "Checking candidate " << candidate.table_name << "." << table->column_name(col_id);

    const auto& soft_key_constraints = table->soft_key_constraints();

    // Skip already discovered unique constraints.
    if (std::any_of(soft_key_constraints.cbegin(), soft_key_constraints.cend(), [&col_id](const auto& key_constraint) {
          const auto& columns = key_constraint.columns();
          return columns.size() == 1 && columns.contains(col_id);
        })) {
      message << " [skipped (already known) in " << format_duration(candidate_time.lap()) << "]";
      Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
      continue;
    }

    resolve_data_type(table->column_data_type(col_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      // utilize efficient check for uniqueness inside each dictionary segment for a potential early out
      if (!_uniqueness_holds_in_dictionary_segments<ColumnDataType>(table, col_id)) {
        message << " [rejected in " << format_duration(candidate_time.lap()) << "]";
        Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
        return;
      }

      // If we reach here, we have to run the more expensive cross-segment duplicate check
      if (!_uniqueness_holds_across_segments<ColumnDataType>(table, col_id)) {
        message << " [rejected in " << format_duration(candidate_time.lap()) << "]";
        Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
        return;
      }

      // We save UCC constraints directly inside the table so they can be forwarded to nodes in a query plan.
      message << " [confirmed in " << format_duration(candidate_time.lap()) << "]";
      Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
      table->add_soft_key_constraint(TableKeyConstraint({col_id}, KeyConstraintType::UNIQUE));
    });
  }
  Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", "Clearing LQP and PQP cache...", LogLevel::Debug);

  Hyrise::get().default_lqp_cache->clear();
  Hyrise::get().default_pqp_cache->clear();
}

template <typename ColumnDataType>
bool UccDiscoveryPlugin::_uniqueness_holds_in_dictionary_segments(std::shared_ptr<Table> table, ColumnID col_id) const {
  const auto chunk_count = table->chunk_count();
  // trigger an early-out if we find the attribute vector of a dictionary segment to be longer than its dictionary
  // an attribute vector longer than the dictionary indicates that at least one duplicate value is contained
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& source_chunk = table->get_chunk(chunk_id);
    // if the chunk does no longer exist, it can't contribute duplicates, so skip it
    if (!source_chunk) {
      continue;
    }
    const auto& source_segment = source_chunk->get_segment(col_id);
    // if the chunk does no longer exist, it can't contribute duplicates, so skip it
    if (!source_segment) {
      continue;
    }

    const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment);
    if (dict_segment) {
      if (dict_segment->dictionary()->size() != dict_segment->attribute_vector()->size()) {
        return false;
      }
    }
  }
  return true;
}

template <typename ColumnDataType>
bool UccDiscoveryPlugin::_uniqueness_holds_across_segments(std::shared_ptr<Table> table, ColumnID col_id) const {
  const auto chunk_count = table->chunk_count();
  // all_values collects the segment values from all chunks.
  auto all_values = std::unordered_set<ColumnDataType>{};
  auto all_values_size = size_t{0};

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& source_chunk = table->get_chunk(chunk_id);
    // if the chunk does no longer exist, it can't contribute duplicates, so skip it
    if (!source_chunk) {
      continue;
    }
    const auto& source_segment = source_chunk->get_segment(col_id);
    // if the chunk does no longer exist, it can't contribute duplicates, so skip it
    if (!source_segment) {
      continue;
    }

    const auto& val_segment = std::dynamic_pointer_cast<ValueSegment<ColumnDataType>>(source_segment);
    const auto& dict_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(source_segment);

    if (val_segment) {
      const auto& values = val_segment->values();

      all_values.insert(begin(values), end(values));
      if (all_values.size() == all_values_size + values.size()) {
        all_values_size += values.size();
      } else {
        // If not all elements have been inserted, there must be a duplicate, so the UCC constraint is violated
        return false;
      }
    } else if (dict_segment) {
      const auto& dict = dict_segment->dictionary();

      all_values.insert(begin(*dict), end(*dict));
      if (all_values.size() == all_values_size + dict->size()) {
        all_values_size += dict->size();
      } else {
        // If not all elements have been inserted, there be a duplicate, so the UCC constraint is violated
        return false;
      }
    } else {
      Fail("The given segment type is not supported for the discovery of UCCs.");
    }
  }
  return true;
}

void UccDiscoveryPlugin::_ucc_candidates_from_aggregate_node(std::shared_ptr<AbstractLQPNode> node,
                                                             UCCCandidates& ucc_candidates) const {
  const auto& aggregate_node = static_cast<AggregateNode&>(*node);
  const auto column_candidates = std::vector<std::shared_ptr<AbstractExpression>>{
      aggregate_node.node_expressions.begin(),
      aggregate_node.node_expressions.begin() + aggregate_node.aggregate_expressions_begin_idx};

  for (const auto& column_candidate : column_candidates) {
    const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(column_candidate);
    if (!lqp_column_expression) {
      continue;
    }
    // every ColumnExpression used as a GroupBy expression should be checked for uniqueness
    const auto stored_table_node =
        std::static_pointer_cast<const StoredTableNode>(lqp_column_expression->original_node.lock());
    ucc_candidates.insert(UCCCandidate{stored_table_node->table_name, lqp_column_expression->original_column_id});
  }
}

void UccDiscoveryPlugin::_ucc_candidates_from_join_node(std::shared_ptr<AbstractLQPNode> node,
                                                        UCCCandidates& ucc_candidates) const {
  const auto& join_node = static_cast<JoinNode&>(*node);
  /* 
   * Fetch the Join Predicate to extract the UCC candidates from.
   * Right now, limited to only equals predicates. 
   * Less than, greater than and not equals predicates could be utilized as well, but the optimizer does not yet 
   * have the capability to rewrite those, therefore the columns are not yet useful candidates.
   */
  const auto& join_predicates = join_node.join_predicates();
  const std::shared_ptr<BinaryPredicateExpression> binary_join_predicate =
      (join_predicates.size() == 1) ? std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates[0])
                                    : nullptr;

  if (!binary_join_predicate) {
    return;
  }
  if (binary_join_predicate->predicate_condition != PredicateCondition::Equals) {
    // Once they can be utilized by rules, also allow <=, >= and != predicate conditions
    return;
  }

  // we only care about semi, inner (both are potential candidates), right outer (left is potential candidate)
  // and left outer (right is potential candidate) joins, as those may be rewritten by optimizer rules right now.
  switch (join_node.join_mode) {
    case JoinMode::Right: {
      // want to check only the left hand side here, as this is the one that will be removed in the end
      const auto subtree_root = join_node.left_input();
      // predicate may be swapped, so get proper operand
      auto column_candidate = std::static_pointer_cast<LQPColumnExpression>(binary_join_predicate->left_operand());
      if (!expression_evaluable_on_lqp(column_candidate, *subtree_root)) {
        column_candidate = std::static_pointer_cast<LQPColumnExpression>(binary_join_predicate->right_operand());
      }

      _ucc_candidates_from_removable_join_input(subtree_root, column_candidate, ucc_candidates);
      return;
    }

    case JoinMode::Inner: {
      const auto column_candidates = {binary_join_predicate->left_operand(), binary_join_predicate->right_operand()};
      for (const auto& column_candidate : column_candidates) {
        const auto lqp_column_expression = std::static_pointer_cast<LQPColumnExpression>(column_candidate);

        // determine which subtree (left or right) belongs to the ColumnExpression
        auto subtree_root = join_node.left_input();
        if (!expression_evaluable_on_lqp(column_candidate, *subtree_root.get())) {
          subtree_root = join_node.right_input();
        }

        // for Join2Semi, this column is already interesting for optimization, add it as a potential candidate
        const auto stored_table_node =
            std::static_pointer_cast<const StoredTableNode>(lqp_column_expression->original_node.lock());
        ucc_candidates.insert(UCCCandidate{stored_table_node->table_name, lqp_column_expression->original_column_id});

        _ucc_candidates_from_removable_join_input(subtree_root, lqp_column_expression, ucc_candidates);
      }
      return;
    }

    case JoinMode::Semi:
    case JoinMode::Left: {
      // want to check only the right hand side here, as this is the one that will be removed in the end
      const auto subtree_root = join_node.right_input();
      // predicate may be swapped, so get proper operand
      auto column_candidate = std::static_pointer_cast<LQPColumnExpression>(binary_join_predicate->right_operand());
      if (!expression_evaluable_on_lqp(column_candidate, *subtree_root.get())) {
        column_candidate = std::static_pointer_cast<LQPColumnExpression>(binary_join_predicate->left_operand());
      }

      _ucc_candidates_from_removable_join_input(subtree_root, column_candidate, ucc_candidates);
      return;
    }

    default: {
      return;
    }
  }
}

void UccDiscoveryPlugin::_ucc_candidates_from_removable_join_input(
    std::shared_ptr<AbstractLQPNode> root_node, std::shared_ptr<LQPColumnExpression> column_candidate,
    UCCCandidates& ucc_candidates) const {
  if (!root_node) {
    // input node may already be nullptr in case we try to get right input of node with only one input
    return;
  } else {
    visit_lqp(root_node, [&](auto& node) {
      if (node->type != LQPNodeType::Predicate) {
        return LQPVisitation::VisitInputs;
      }

      // when looking at predicate node, check whether the searched column is filtered in this predicate
      // -> if so, it is a valid UCC candidate; if not, still continue search
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

      // first, ensure that we look at a binary predicate expression checking for equality (e.g., A==B)
      const auto predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node->predicate());
      if (!predicate || predicate->predicate_condition != PredicateCondition::Equals) {
        return LQPVisitation::VisitInputs;
      }

      // get the column expression, should be left, but also check the right operand if the left one is not column
      auto column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->left_operand());
      auto value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->right_operand());
      if (!column_expression) {
        column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->right_operand());
        value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->left_operand());
      }

      if (!column_expression || !value_expression) {
        // predicate needs to look like column = value or value = column; if not, move on
        return LQPVisitation::VisitInputs;
      }

      const auto expression_table =
          std::static_pointer_cast<const StoredTableNode>(column_expression->original_node.lock());
      const auto candidate_table =
          std::static_pointer_cast<const StoredTableNode>(column_candidate->original_node.lock());

      if (expression_table == candidate_table) {
        // both columns same table -> if both are UCC we could still convert join -> both are UCC candidate
        ucc_candidates.insert(UCCCandidate{expression_table->table_name, column_expression->original_column_id});
        ucc_candidates.insert(UCCCandidate{candidate_table->table_name, column_candidate->original_column_id});
      }

      return LQPVisitation::VisitInputs;
    });
  }
}

EXPORT_PLUGIN(UccDiscoveryPlugin)

}  // namespace hyrise

namespace std {

size_t hash<hyrise::UCCCandidate>::operator()(const hyrise::UCCCandidate& uc) const {
  return uc.hash();
}

}  // namespace std
