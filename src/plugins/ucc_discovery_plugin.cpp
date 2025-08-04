#include "ucc_discovery_plugin.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/container_hash/hash.hpp>

// NOLINTNEXTLINE(misc-include-cleaner): We access methods of AbstractBenchmarkItemRunner in `pre_benchmark_hook()`.
#include "../benchmarklib/abstract_benchmark_item_runner.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "resolve_type.hpp"
#include "storage/constraints/constraint_utils.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/abstract_plugin.hpp"
#include "utils/assert.hpp"
#include "utils/log_manager.hpp"
#include "utils/timer.hpp"

namespace hyrise {

UccCandidate::UccCandidate(const ObjectID init_table_id, const ColumnID init_column_id)
    : table_id{init_table_id}, column_id{init_column_id} {}

bool UccCandidate::operator==(const UccCandidate& other) const {
  return (column_id == other.column_id) && (table_id == other.table_id);
}

bool UccCandidate::operator!=(const UccCandidate& other) const {
  return !(other == *this);
}

size_t UccCandidate::hash() const {
  auto hash = size_t{0};
  boost::hash_combine(hash, table_id);
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
  return {{"DiscoverUCCs", [&]() {
             _validate_ucc_candidates(_identify_ucc_candidates());
           }}};
}

std::optional<PreBenchmarkHook> UccDiscoveryPlugin::pre_benchmark_hook() {
  return [&](auto& benchmark_item_runner) {
    for (const auto item_id : benchmark_item_runner.items()) {
      benchmark_item_runner.execute_item(item_id);
    }
    _validate_ucc_candidates(_identify_ucc_candidates());
  };
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
  const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::Yes);
  const auto current_commit_id = transaction_context->snapshot_commit_id();

  for (const auto& candidate : ucc_candidates) {
    auto candidate_timer = Timer();
    const auto table = Hyrise::get().storage_manager.get_table(candidate.table_id);
    const auto column_id = candidate.column_id;

    auto message = std::stringstream{};

    message << "Checking candidate " << Hyrise::get().catalog.table_name(candidate.table_id) << "." << table->column_name(column_id);

    const auto& soft_key_constraints = table->soft_key_constraints();
    const auto candidate_columns = std::set<ColumnID>{column_id};
    const auto existing_ucc =
        std::find_if(soft_key_constraints.cbegin(), soft_key_constraints.cend(), [&](const auto& key_constraint) {
          return std::ranges::includes(key_constraint.columns(), candidate_columns);
        });

    if (existing_ucc != soft_key_constraints.end()) {
      // Check if the found key constraint is a primary key constraint. If it is, we can directly skip the candidate.
      if (existing_ucc->key_type() == KeyConstraintType::PRIMARY_KEY) {
        message << " [skipped (already known to be a primary key) in " << candidate_timer.lap_formatted() << "]";
        Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
        continue;
      }

      // Check if MVCC data tells us that the existing UCC is guaranteed to be valid or invalid. If it is, we can
      // skip the expensive revalidation attempt of the UCC. This also covers the case where the existing UCC is
      // schema-given.
      // We do not update the CommitID of the existing UCC. This could lead to currently running transactions not being
      // able to see the updated UCC anymore. Even though the UCC was guaranteed to be valid, the new CommitID is
      // larger so we lose the knowledge about this previous guarantee.
      if (existing_ucc->key_type() == KeyConstraintType::UNIQUE) {
        if (key_constraint_is_confidently_valid(table, *existing_ucc)) {
          message << " [skipped (already known and guaranteed to be still VALID) in " << candidate_timer.lap_formatted()
                  << "]";
          Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
          continue;
        }
        if (key_constraint_is_confidently_invalid(table, *existing_ucc)) {
          message << " [skipped (already known and guaranteed to be INVALID) in " << candidate_timer.lap_formatted()
                  << "]";
          Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
          continue;
        }
      }
    }

    // If no UCC already exists or the existing UCC is not guaranteed to be still valid, we have to now (re-)validate
    // the UCC candidate.
    resolve_data_type(table->column_data_type(column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      // Utilize efficient check for uniqueness inside each dictionary segment for a potential early out.
      // If that does not allow us to reject the UCC right away, we have to run the more expensive
      // cross-segment duplicate check (next clause of the if-condition).
      if (_dictionary_segments_contain_duplicates<ColumnDataType>(table, column_id) ||
          !_uniqueness_holds_across_segments<ColumnDataType>(table, candidate.table_id, column_id,
                                                             transaction_context)) {
        message << " [rejected in " << candidate_timer.lap_formatted() << "]";

        auto [existing_key_constraint, inserted] = table->_table_key_constraints.insert(
            TableKeyConstraint{{column_id}, KeyConstraintType::UNIQUE, UNSET_COMMIT_ID, current_commit_id});

        if (!inserted) {
          // If the constraint was not inserted, we need to update the existing one.
          existing_key_constraint->invalidated_on(current_commit_id);
        }
      } else {
        message << " [confirmed in " << candidate_timer.lap_formatted() << "]";

        auto [existing_key_constraint, inserted] = table->_table_key_constraints.insert(
            TableKeyConstraint{{column_id}, KeyConstraintType::UNIQUE, current_commit_id, MAX_COMMIT_ID});

        if (!inserted) {
          // If the constraint was not inserted, we need to update the existing one.
          existing_key_constraint->revalidated_on(current_commit_id);
        }
      }
      Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", message.str(), LogLevel::Info);
    });
  }
  Hyrise::get().log_manager.add_message("UccDiscoveryPlugin", "Clearing LQP and PQP cache...", LogLevel::Debug);

  Hyrise::get().default_lqp_cache->clear();
  Hyrise::get().default_pqp_cache->clear();
}

template <typename ColumnDataType>
bool UccDiscoveryPlugin::_dictionary_segments_contain_duplicates(const std::shared_ptr<const Table>& table,
                                                                 const ColumnID column_id) {
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

    if (source_chunk->invalid_row_count() != 0) {
      // The segment might have been modified. Because the modification might consist of deleting a duplicate value,
      // we can't be sure that the segment still has duplicates using the heuristic employed below.
      return false;
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
bool UccDiscoveryPlugin::_uniqueness_holds_across_segments(
    const std::shared_ptr<const Table>& table, const ObjectID table_id, const ColumnID column_id,
    const std::shared_ptr<TransactionContext>& transaction_context) {
  // `distinct_values_across_segments` collects the segment values from all chunks.
  auto distinct_values_across_segments = std::unordered_set<ColumnDataType>{};
  auto unmodified_chunks = std::vector<ChunkID>{};

  const auto chunk_count = table->chunk_count();

  // For all segments, we now want to verify if they contain duplicates. If a segment does not add the expected number
  // of distinct values, we know that a duplicate exists in the segment / column and can return early. We start by
  // handling the unmodified dictionary segments.
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto source_chunk = table->get_chunk(chunk_id);
    if (!source_chunk) {
      // If this chunk does not exist, we do not need to check it with the validate operator later either.
      unmodified_chunks.push_back(chunk_id);
      continue;
    }
    const auto source_segment = source_chunk->get_segment(column_id);
    // Were values deleted or updated in this chunk? If not, we can directly use range insertion.
    if ((source_chunk->invalid_row_count() == 0 && !source_chunk->is_mutable())) {
      // If we enter this branch, we know that this segment has not been modified since its creation. Therefore we can
      // directly add all of its values to the set of distinct values.

      // The set of distinct values across all segments should grow by the number of rows in the segment. Otherwise,
      // some value must have been inserted twice, and we detected a duplicate that violates the UCC.
      const auto expected_distinct_value_count = distinct_values_across_segments.size() + source_segment->size();

      const auto dictionary_segment =
          std::dynamic_pointer_cast<const DictionarySegment<ColumnDataType>>(source_segment);
      const auto value_segment = std::dynamic_pointer_cast<const ValueSegment<ColumnDataType>>(source_segment);
      if (dictionary_segment) {
        // Directly insert dictionary entries.
        const auto& dictionary = dictionary_segment->dictionary();
        distinct_values_across_segments.insert(dictionary->cbegin(), dictionary->cend());
      } else if (value_segment && !value_segment->is_nullable()) {
        const auto& values = value_segment->values();
        distinct_values_across_segments.insert(values.cbegin(), values.cend());
      } else {
        continue;
      }

      if (distinct_values_across_segments.size() != expected_distinct_value_count) {
        return false;
      }

      // If we managed to check this segment already, we don't need to do it again later using the validate operator.
      unmodified_chunks.push_back(chunk_id);
    }
  }

  // Using the validate operator, we get a view of the current content of the table, filtering out overwritten and
  // deleted values.
  const auto get_table = std::make_shared<GetTable>(table_id, unmodified_chunks, std::vector<ColumnID>());
  get_table->execute();
  const auto validate_table = std::make_shared<Validate>(get_table);
  validate_table->set_transaction_context(transaction_context);
  validate_table->execute();
  const auto& table_view = validate_table->get_output();

  // Check all chunks that we could not handle in the previous loop. Note that the loop below will only contain these
  // chunks, as we have excluded the others when executing the GetTable operator.
  const auto validated_chunk_count = table_view->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < validated_chunk_count; ++chunk_id) {
    const auto source_chunk = table_view->get_chunk(chunk_id);
    const auto source_segment = source_chunk->get_segment(column_id);

    const auto expected_distinct_value_count = distinct_values_across_segments.size() + source_segment->size();
    auto running_distinct_value_count = distinct_values_across_segments.size();
    segment_with_iterators<ColumnDataType>(*source_segment, [&](auto it, const auto end) {
      while (it != end) {
        if (it->is_null()) {
          break;
        }
        distinct_values_across_segments.insert(it->value());
        if (running_distinct_value_count + 1 != distinct_values_across_segments.size()) {
          break;
        }
        ++running_distinct_value_count;
        ++it;
      }
    });

    // See explanation on the `expected_distinct_value_count` in the first loop.
    if (distinct_values_across_segments.size() != expected_distinct_value_count) {
      return false;
    }
  }

  // Since we did not return earlier, we are now sure that there are no duplicates in the column.
  return true;
}

void UccDiscoveryPlugin::_ucc_candidates_from_aggregate_node(const std::shared_ptr<const AbstractLQPNode>& node,
                                                             UccCandidates& ucc_candidates) {
  const auto& aggregate_node = static_cast<const AggregateNode&>(*node);
  const auto column_candidates = std::vector<std::shared_ptr<AbstractExpression>>{
      aggregate_node.node_expressions.cbegin(),
      aggregate_node.node_expressions.cbegin() + static_cast<int64_t>(aggregate_node.aggregate_expressions_begin_idx)};

  for (const auto& column_candidate : column_candidates) {
    const auto lqp_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(column_candidate);
    if (!lqp_column_expression) {
      continue;
    }
    // Every ColumnExpression used as a GroupBy expression should be checked for uniqueness.
    const auto& stored_table_node = static_cast<const StoredTableNode&>(*lqp_column_expression->original_node.lock());
    ucc_candidates.insert(UccCandidate{stored_table_node.table_id, lqp_column_expression->original_column_id});
  }
}

void UccDiscoveryPlugin::_ucc_candidates_from_join_node(const std::shared_ptr<const AbstractLQPNode>& node,
                                                        UccCandidates& ucc_candidates) {
  const auto& join_node = static_cast<const JoinNode&>(*node);
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
        ucc_candidates.insert(UccCandidate{stored_table_node.table_id, lqp_column_expression->original_column_id});

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
    const std::shared_ptr<const AbstractLQPNode>& root_node,
    const std::shared_ptr<const LQPColumnExpression>& column_candidate, UccCandidates& ucc_candidates) {
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
      ucc_candidates.emplace(expression_table_node->table_id, column_expression->original_column_id);
      ucc_candidates.emplace(candidate_table_node->table_id, column_candidate->original_column_id);
    }

    return LQPVisitation::VisitInputs;
  });
}

EXPORT_PLUGIN(UccDiscoveryPlugin);

}  // namespace hyrise

namespace std {

size_t hash<hyrise::UccCandidate>::operator()(const hyrise::UccCandidate& ucc_candidate) const {
  return ucc_candidate.hash();
}

}  // namespace std
