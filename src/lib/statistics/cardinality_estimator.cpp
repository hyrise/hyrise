#include "cardinality_estimator.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <optional>
#include <ostream>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/variant/get.hpp>

#include "all_parameter_variant.hpp"
#include "all_type_variant.hpp"
#include "attribute_statistics.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/logical_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "logical_query_plan/window_node.hpp"
#include "lossy_cast.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/join_graph_statistics_cache.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram_builder.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

namespace {

// Magic constants used in places where a better estimation would be implementable (either with
// statistics objects not yet implemented or new algorithms) - but doing so just wasn't warranted yet.
constexpr auto PLACEHOLDER_SELECTIVITY_LOW = 0.1;
constexpr auto PLACEHOLDER_SELECTIVITY_MEDIUM = 0.5;
constexpr auto PLACEHOLDER_SELECTIVITY_HIGH = 0.9;
constexpr auto PLACEHOLDER_SELECTIVITY_ALL = 1.0;

template <typename T>
std::optional<Selectivity> estimate_null_value_ratio_of_column(const TableStatistics& table_statistics,
                                                               const AttributeStatistics<T>& column_statistics) {
  // If the column has an explicit null value ratio associated with it, we can just use that
  if (column_statistics.null_value_ratio) {
    return column_statistics.null_value_ratio->ratio;
  }

  // Otherwise derive the null value ratio from the total count of a histogram (which excludes NULLs) and the
  // TableStatistics row count (which includes NULLs)
  if (column_statistics.histogram && table_statistics.row_count != 0.0) {
    return 1.0 - (column_statistics.histogram->total_count() / table_statistics.row_count);
  }

  return std::nullopt;
}

std::shared_ptr<TableStatistics> prune_column_statistics(
    const std::shared_ptr<TableStatistics>& table_statistics, const std::vector<ColumnID>& pruned_column_ids,
    const std::vector<std::shared_ptr<AbstractExpression>>& output_expressions,
    std::optional<ExpressionUnorderedSet>& required_columns) {
  if (!required_columns && pruned_column_ids.empty()) {
    return table_statistics;
  }

  // Prune `pruned_column_ids` and all statistics for unused columns from the statistics.
  const auto column_count = table_statistics->column_statistics.size();
  auto output_column_statistics =
      std::vector<std::shared_ptr<const BaseAttributeStatistics>>(column_count - pruned_column_ids.size());

  auto pruned_column_ids_iter = pruned_column_ids.begin();
  for (auto input_column_id = ColumnID{0}, output_column_id = ColumnID{0}; input_column_id < column_count;
       ++input_column_id) {
    // Skip `stored_column_id` if it is in the sorted vector `pruned_column_ids`.
    if (pruned_column_ids_iter != pruned_column_ids.end() && input_column_id == *pruned_column_ids_iter) {
      ++pruned_column_ids_iter;
      continue;
    }

    // Create a dummy statistics object if the column is not actually used for estimations.
    const auto& expression = output_expressions[output_column_id];
    if (required_columns && !required_columns->contains(expression)) {
      output_column_statistics[output_column_id] =
          std::make_shared<CardinalityEstimator::DummyStatistics>(expression->data_type());
    } else {
      output_column_statistics[output_column_id] = table_statistics->column_statistics[input_column_id];
    }
    ++output_column_id;
  }

  return std::make_shared<TableStatistics>(std::move(output_column_statistics), table_statistics->row_count);
}

}  // namespace

CardinalityEstimator::DummyStatistics::DummyStatistics(const DataType init_data_type)
    : BaseAttributeStatistics(init_data_type) {}

void CardinalityEstimator::DummyStatistics::set_statistics_object(
    const std::shared_ptr<const AbstractStatisticsObject>& /*statistics_object*/) {
  Fail("DummyStatistics do not manage statistics objects.");
}

std::shared_ptr<const BaseAttributeStatistics> CardinalityEstimator::DummyStatistics::scaled(
    const Selectivity /*selectivity*/) const {
  return shared_from_this();
}

std::shared_ptr<const BaseAttributeStatistics> CardinalityEstimator::DummyStatistics::sliced(
    const PredicateCondition /*predicate_condition*/, const AllTypeVariant& /*variant_value*/,
    const std::optional<AllTypeVariant>& /*variant_value2*/) const {
  Fail("DummyStatistics should not be used for actual estimations. Was the required column accidentally pruned?");
}

std::ostream& operator<<(std::ostream& stream, const CardinalityEstimator::DummyStatistics& /*dummy_statistics*/) {
  return stream << "DummyStatistics";
}

void CardinalityEstimator::prune_unused_statistics() const {
  Assert(cardinality_estimation_cache.statistics_by_lqp, "Statistics pruning requires caching.");
  cardinality_estimation_cache.required_column_expressions.emplace();
}

void CardinalityEstimator::do_not_prune_unused_statistics() const {
  cardinality_estimation_cache.required_column_expressions.reset();
}

// Ensure that the available statistics required to estimate a node are provided, unless they are aggregates etc., which
// we currently do not estimate.
void CardinalityEstimator::assert_required_statistics(const ColumnID column_id,
                                                      const std::shared_ptr<AbstractLQPNode>& input_node,
                                                      const std::shared_ptr<const TableStatistics>& input_statistics) {
  const auto& column_statistics = input_statistics->column_statistics[column_id];
  Assert(column_statistics, "Expected input statistics.");
  const auto* const is_dummy_object = dynamic_cast<const DummyStatistics*>(&*column_statistics);

  // Case (i): There might not be statistics available if the required expression is not an LQPColumnExpression.
  const auto input_expression = input_node->output_expressions()[column_id];
  if (input_expression->type != ExpressionType::LQPColumn) {
    // Fail if we have statistics for expressions other than LQPColumnExpressions. Thus, we notice if we add estimates
    // for them but forget to add assertions here.
    Assert(is_dummy_object, "There are statistics for an expression other than LQPColumnExpression. Add a check!");
    return;
  }

  // Case (ii): If input statistics are available, everything is fine.
  if (!is_dummy_object) {
    return;
  }

  // Case (iii): Even for columns, original statistics might not be set (e.g., for StaticTableNode or in tests).
  auto base_statistics = std::shared_ptr<TableStatistics>{};
  const auto& column_expression = static_cast<const LQPColumnExpression&>(*input_expression);
  const auto original_node = column_expression.original_node.lock();
  Assert(original_node, "LQPColumnExpression's original node expired. LQP is invalid.");
  switch (original_node->type) {
    case LQPNodeType::Mock: {
      const auto& mock_node = static_cast<const MockNode&>(*original_node);
      base_statistics = mock_node.table_statistics();
    } break;
    case LQPNodeType::StaticTable:
      base_statistics = static_cast<const StaticTableNode&>(*original_node).table->table_statistics();
      break;
    case LQPNodeType::StoredTable: {
      const auto& stored_table_node = static_cast<const StoredTableNode&>(*original_node);
      if (stored_table_node.table_statistics) {
        base_statistics = stored_table_node.table_statistics;
        break;
      }

      const auto table = Hyrise::get().storage_manager.get_table(stored_table_node.table_name);
      base_statistics = table->table_statistics();
    } break;
    default:
      Fail("Unexpected orignal node for LQPColumnExpression.");
  }

  if (!base_statistics) {
    return;
  }

  const auto original_column_id = column_expression.original_column_id;
  Assert(base_statistics->column_statistics.size() > original_column_id, "TableStatistics miss columns.");
  Assert(!base_statistics->column_statistics[original_column_id],
         "Missing AttributeStatistics for required column. Have they been pruned?");
}

std::shared_ptr<CardinalityEstimator> CardinalityEstimator::new_instance() {
  return std::make_shared<CardinalityEstimator>();
}

Cardinality CardinalityEstimator::estimate_cardinality(const std::shared_ptr<const AbstractLQPNode>& lqp,
                                                       const bool cacheable) const {
  auto statistics_cache = StatisticsByLQP{};
  return estimate_statistics(lqp, cacheable, statistics_cache)->row_count;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_statistics(
    const std::shared_ptr<const AbstractLQPNode>& lqp, const bool cacheable) const {
  auto statistics_cache = StatisticsByLQP{};
  return estimate_statistics(lqp, cacheable, statistics_cache);
}

void CardinalityEstimator::guarantee_join_graph(const JoinGraph& join_graph) const {
  cardinality_estimation_cache.join_graph_statistics_cache.emplace(
      JoinGraphStatisticsCache::from_join_graph(join_graph));
}

void CardinalityEstimator::guarantee_bottom_up_construction(const std::shared_ptr<const AbstractLQPNode>& lqp) const {
  cardinality_estimation_cache.statistics_by_lqp.emplace();
  // Turn on statistics pruning, but do not populate the required columns yet. We do that lazily when we perform the
  // first estimations. This helps to avoid overhead for optimizer rules that do not perform the anticipated rewrites
  // because they handle cases that are not present in the LQP (e.g., in TPC-C).
  prune_unused_statistics();
  cardinality_estimation_cache.lqp = lqp;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_statistics(
    const std::shared_ptr<const AbstractLQPNode>& lqp, const bool cacheable, StatisticsByLQP& statistics_cache) const {
  /**
   * 1. Try a cache lookup for requested LQP.
   *
   * The `join_graph_bitmask` is kept so that if cache lookup fails, a new cache entry with this bitmask as a key can
   * be created at the end of this function.
   *
   * Lookup in `join_graph_statistics_cache` is expected to have a higher hit rate (since every bitmask represents
   * multiple LQPs) than `statistics_by_lqp`, but the lookup is also more expensive. Thus, the lookup in
   * `join_graph_statistics_cache` is performed last.
   *
   * Also try a lookup in the temporary cache used for the estimation of a single LQP.
   */
  if (cardinality_estimation_cache.statistics_by_lqp) {
    const auto plan_statistics_iter = cardinality_estimation_cache.statistics_by_lqp->find(lqp);
    if (plan_statistics_iter != cardinality_estimation_cache.statistics_by_lqp->end()) {
      return plan_statistics_iter->second;
    }
  }

  const auto cached_statistics = statistics_cache.find(lqp);
  if (cached_statistics != statistics_cache.end()) {
    return cached_statistics->second;
  }

  auto join_graph_bitmask = std::optional<JoinGraphStatisticsCache::Bitmask>{};
  if (cardinality_estimation_cache.join_graph_statistics_cache) {
    join_graph_bitmask = cardinality_estimation_cache.join_graph_statistics_cache->bitmask(lqp);
    if (join_graph_bitmask) {
      auto cached_statistics =
          cardinality_estimation_cache.join_graph_statistics_cache->get(*join_graph_bitmask, lqp->output_expressions());
      if (cached_statistics) {
        return cached_statistics;
      }
    } else {
      // The LQP is not (a subgraph of) a JoinGraph and therefore we cannot use the JoinGraphStatisticsCache.
    }
  }

  /**
   * 2. Cache lookup failed - perform an actual cardinality estimation.
   */
  auto output_table_statistics = std::shared_ptr<TableStatistics>{};
  auto& required_columns = cardinality_estimation_cache.required_column_expressions;
  const auto left_input_table_statistics =
      lqp->left_input() ? estimate_statistics(lqp->left_input(), cacheable, statistics_cache) : nullptr;
  const auto right_input_table_statistics =
      lqp->right_input() ? estimate_statistics(lqp->right_input(), cacheable, statistics_cache) : nullptr;

  switch (lqp->type) {
    case LQPNodeType::Aggregate: {
      const auto& aggregate_node = static_cast<const AggregateNode&>(*lqp);
      output_table_statistics = estimate_aggregate_node(aggregate_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Alias: {
      const auto& alias_node = static_cast<const AliasNode&>(*lqp);
      output_table_statistics = estimate_alias_node(alias_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Join: {
      const auto& join_node = static_cast<const JoinNode&>(*lqp);
      output_table_statistics =
          estimate_join_node(join_node, left_input_table_statistics, right_input_table_statistics);
    } break;

    case LQPNodeType::Limit: {
      const auto& limit_node = static_cast<const LimitNode&>(*lqp);
      output_table_statistics = estimate_limit_node(limit_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Mock: {
      const auto& mock_node = static_cast<const MockNode&>(*lqp);
      Assert(mock_node.table_statistics(), "Cannot return statistics of MockNode that was not assigned statistics.");
      _populate_required_column_expressions();
      output_table_statistics = prune_column_statistics(mock_node.table_statistics(), mock_node.pruned_column_ids(),
                                                        mock_node.output_expressions(), required_columns);
    } break;

    case LQPNodeType::Predicate: {
      const auto& predicate_node = static_cast<const PredicateNode&>(*lqp);
      output_table_statistics =
          estimate_predicate_node(predicate_node, left_input_table_statistics, cacheable, statistics_cache);
    } break;

    case LQPNodeType::Projection: {
      const auto& projection_node = static_cast<const ProjectionNode&>(*lqp);
      output_table_statistics = estimate_projection_node(projection_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Sort: {
      output_table_statistics = left_input_table_statistics;
    } break;

    case LQPNodeType::StaticTable: {
      const auto& static_table_node = static_cast<const StaticTableNode&>(*lqp);
      const auto& table_statistics = static_table_node.table->table_statistics();
      // StaticTableNodes may or may not provide statistics. If there are statistics, prune and forward them.
      if (table_statistics) {
        _populate_required_column_expressions();
        output_table_statistics = prune_column_statistics(table_statistics, std::vector<ColumnID>{},
                                                          static_table_node.output_expressions(), required_columns);
        break;
      }

      // If there are no statistics, provide dummy statistics.
      const auto cardinality = static_cast<Cardinality>(static_table_node.table->row_count());
      const auto& column_definitions = static_table_node.table->column_definitions();
      auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>{};
      column_statistics.reserve(column_definitions.size());
      for (const auto& column_definition : column_definitions) {
        column_statistics.emplace_back(std::make_shared<DummyStatistics>(column_definition.data_type));
      }

      output_table_statistics = std::make_shared<TableStatistics>(std::move(column_statistics), cardinality);
    } break;

    case LQPNodeType::StoredTable: {
      const auto& stored_table_node = static_cast<const StoredTableNode&>(*lqp);

      const auto& stored_table = Hyrise::get().storage_manager.get_table(stored_table_node.table_name);
      Assert(stored_table->table_statistics(), "Stored Table should have cardinality estimation statistics");

      auto table_statistics = std::shared_ptr<TableStatistics>{};
      if (stored_table_node.table_statistics) {
        // TableStatistics have changed from the original table's statistics
        Assert(stored_table_node.table_statistics->column_statistics.size() ==
                   static_cast<size_t>(stored_table->column_count()),
               "Statistics in StoredTableNode should have same number of columns as original table");
        Assert(stored_table_node.table_statistics->row_count >= 0, "Tables can't have negative row counts");
        table_statistics = stored_table_node.table_statistics;
      } else {
        table_statistics = stored_table->table_statistics();
      }
      _populate_required_column_expressions();
      output_table_statistics = prune_column_statistics(table_statistics, stored_table_node.pruned_column_ids(),
                                                        stored_table_node.output_expressions(), required_columns);
    } break;

    case LQPNodeType::Validate: {
      const auto& validate_node = static_cast<const ValidateNode&>(*lqp);
      output_table_statistics = estimate_validate_node(validate_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Union: {
      const auto& union_node = static_cast<const UnionNode&>(*lqp);
      output_table_statistics =
          estimate_union_node(union_node, left_input_table_statistics, right_input_table_statistics);
    } break;

    case LQPNodeType::Window: {
      const auto& window_node = static_cast<const WindowNode&>(*lqp);
      output_table_statistics = estimate_window_node(window_node, left_input_table_statistics);
    } break;

    // Currently, there is no actual estimation being done and we always apply the worst case.
    case LQPNodeType::Intersect:
    case LQPNodeType::Except: {
      output_table_statistics = left_input_table_statistics;
    } break;

    // These Node types should not be relevant during query optimization. Return an empty TableStatistics object for
    // them.
    case LQPNodeType::CreateTable:
    case LQPNodeType::CreatePreparedPlan:
    case LQPNodeType::CreateView:
    case LQPNodeType::Update:
    case LQPNodeType::Insert:
    case LQPNodeType::Import:
    case LQPNodeType::Export:
    case LQPNodeType::Delete:
    case LQPNodeType::DropView:
    case LQPNodeType::DropTable:
    case LQPNodeType::ChangeMetaTable:
    case LQPNodeType::DummyTable: {
      auto empty_column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>{};
      output_table_statistics = std::make_shared<TableStatistics>(std::move(empty_column_statistics), Cardinality{0});
    } break;

    case LQPNodeType::Root:
      Fail("Cardinality of a node of this type should never be requested.");
  }

  /**
   * 3. Store output_table_statistics in cache.
   */
  if (join_graph_bitmask && cacheable) {
    cardinality_estimation_cache.join_graph_statistics_cache->set(*join_graph_bitmask, lqp->output_expressions(),
                                                                  output_table_statistics);
  }

  if (cardinality_estimation_cache.statistics_by_lqp && cacheable) {
    cardinality_estimation_cache.statistics_by_lqp->emplace(lqp, output_table_statistics);
  }

  // We can always cache statistics during a single invocation, which is useful for diamonds in the plan.
  statistics_cache.emplace(lqp, output_table_statistics);

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_alias_node(
    const AliasNode& alias_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For AliasNodes, just reorder/remove AttributeStatistics from the input.

  const auto& output_expressions = alias_node.output_expressions();
  const auto output_expression_count = output_expressions.size();
  const auto& input_expressions = alias_node.left_input()->output_expressions();
  auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>{output_expression_count};

  for (auto expression_idx = ColumnID{0}; expression_idx < output_expression_count; ++expression_idx) {
    const auto& expression = *output_expressions[expression_idx];
    const auto input_column_id = find_expression_idx(expression, input_expressions);
    Assert(input_column_id, "Could not resolve " + expression.as_column_name());
    column_statistics[expression_idx] = input_table_statistics->column_statistics[*input_column_id];
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), input_table_statistics->row_count);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_projection_node(
    const ProjectionNode& projection_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For ProjectionNodes, reorder/remove AttributeStatistics from the input. They also perform calculations creating new
  // colums.
  // TODO(anybody): For columns newly created by a Projection no meaningful statistics can be generated yet, hence an
  //               empty AttributeStatistics object is created.

  const auto& output_expressions = projection_node.output_expressions();
  const auto output_expression_count = output_expressions.size();
  const auto& input_expressions = projection_node.left_input()->output_expressions();
  auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>{output_expression_count};

  for (auto expression_idx = ColumnID{0}; expression_idx < output_expression_count; ++expression_idx) {
    const auto& expression = *output_expressions[expression_idx];
    const auto input_column_id = find_expression_idx(expression, input_expressions);
    if (input_column_id) {
      column_statistics[expression_idx] = input_table_statistics->column_statistics[*input_column_id];
    } else {
      column_statistics[expression_idx] = std::make_shared<DummyStatistics>(expression.data_type());
    }
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), input_table_statistics->row_count);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_window_node(
    const WindowNode& window_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // Forward the input statistics for all but the last column (which contains the window function result).
  const auto& output_expressions = window_node.output_expressions();
  const auto output_expression_count = output_expressions.size();
  auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>{output_expression_count};
  const auto forwarded_expression_count = output_expression_count - 1;

  for (auto column_id = ColumnID{0}; column_id < forwarded_expression_count; ++column_id) {
    column_statistics[column_id] = input_table_statistics->column_statistics[column_id];
  }

  // For the result of the window function, dummy statistics are created for now.
  column_statistics[forwarded_expression_count] =
      std::make_shared<DummyStatistics>(output_expressions.back()->data_type());

  return std::make_shared<TableStatistics>(std::move(column_statistics), input_table_statistics->row_count);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_aggregate_node(
    const AggregateNode& aggregate_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For AggregateNodes, statistics from group-by columns are forwarded and for the aggregate columns
  // dummy statistics are created for now.

  const auto& output_expressions = aggregate_node.output_expressions();
  const auto output_expression_count = output_expressions.size();
  const auto& input_expressions = aggregate_node.left_input()->output_expressions();
  auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>{output_expression_count};

  for (auto expression_idx = ColumnID{0}; expression_idx < output_expression_count; ++expression_idx) {
    const auto& expression = *output_expressions[expression_idx];
    const auto input_column_id = find_expression_idx(expression, input_expressions);
    if (input_column_id) {
      column_statistics[expression_idx] = input_table_statistics->column_statistics[*input_column_id];
    } else {
      column_statistics[expression_idx] = std::make_shared<DummyStatistics>(expression.data_type());
    }
  }

  // AggregateNodes without GROUP BY columns always return a single row.
  const auto cardinality =
      aggregate_node.aggregate_expressions_begin_idx == 0 ? Cardinality{1} : input_table_statistics->row_count;

  return std::make_shared<TableStatistics>(std::move(column_statistics), cardinality);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_validate_node(
    const ValidateNode& /*validate_node*/, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // Currently no statistics available to base ValidateNode on
  return input_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_predicate_node(
    const PredicateNode& predicate_node, const std::shared_ptr<TableStatistics>& input_table_statistics,
    const bool cacheable, StatisticsByLQP& statistics_cache) const {
  // For PredicateNodes, the statistics of the columns scanned on are sliced and all other columns' statistics are
  // scaled with the estimated selectivity of the predicate.

  const auto predicate = predicate_node.predicate();

  if (const auto logical_expression = std::dynamic_pointer_cast<LogicalExpression>(predicate)) {
    if (logical_expression->logical_operator == LogicalOperator::Or) {
      // For now, we handle OR by assuming that predicates do not overlap, i.e., by adding the selectivities.

      const auto left_predicate_node =
          PredicateNode::make(logical_expression->left_operand(), predicate_node.left_input());
      const auto left_statistics =
          estimate_predicate_node(*left_predicate_node, input_table_statistics, cacheable, statistics_cache);

      const auto right_predicate_node =
          PredicateNode::make(logical_expression->right_operand(), predicate_node.left_input());
      const auto right_statistics =
          estimate_predicate_node(*right_predicate_node, input_table_statistics, cacheable, statistics_cache);

      const auto row_count = Cardinality{
          std::min(left_statistics->row_count + right_statistics->row_count, input_table_statistics->row_count)};

      auto selectivity = Selectivity{1};
      if (input_table_statistics->row_count > 0) {
        selectivity = row_count / input_table_statistics->row_count;
      }

      auto output_column_statistics =
          std::vector<std::shared_ptr<const BaseAttributeStatistics>>{input_table_statistics->column_statistics.size()};

      for (auto column_id = ColumnID{0}; column_id < output_column_statistics.size(); ++column_id) {
        output_column_statistics[column_id] = input_table_statistics->column_statistics[column_id]->scaled(selectivity);
      }

      auto output_table_statistics = std::make_shared<TableStatistics>(std::move(output_column_statistics), row_count);

      return output_table_statistics;
    }

    if (logical_expression->logical_operator == LogicalOperator::And) {
      // Estimate AND by splitting it up into two consecutive PredicateNodes

      const auto first_predicate_node =
          PredicateNode::make(logical_expression->left_operand(), predicate_node.left_input());
      const auto first_predicate_statistics =
          estimate_predicate_node(*first_predicate_node, input_table_statistics, cacheable, statistics_cache);

      const auto second_predicate_node = PredicateNode::make(logical_expression->right_operand(), first_predicate_node);
      auto second_predicate_statistics =
          estimate_predicate_node(*second_predicate_node, first_predicate_statistics, cacheable, statistics_cache);

      return second_predicate_statistics;
    }
  }

  // Estimating correlated parameters is tricky. Example:
  //   SELECT c_custkey, (SELECT AVG(o_totalprice) FROM orders WHERE o_custkey = c_custkey) FROM customer
  // If the subquery was executed for each customer row, assuming that the predicate has a selectivity matching that
  // of searching for a single value would be reasonable. However, it is likely that the SubqueryToJoinRule will
  // rewrite this query so that the CorrelatedParameterExpression will turn into an LQPColumnExpression that is part
  // of a join predicate. However, since the JoinOrderingRule is executed before the SubqueryToJoinRule, it would
  // create a different join order if it assumes `orders` to be filtered down to very few values. For now, we return
  // PLACEHOLDER_SELECTIVITY_HIGH. This is not perfect, but better than estimating `num_rows / distinct_values`.
  if (expression_contains_correlated_parameter(predicate)) {
    auto output_column_statistics =
        std::vector<std::shared_ptr<const BaseAttributeStatistics>>{input_table_statistics->column_statistics.size()};

    for (auto column_id = ColumnID{0}; column_id < output_column_statistics.size(); ++column_id) {
      output_column_statistics[column_id] =
          input_table_statistics->column_statistics[column_id]->scaled(PLACEHOLDER_SELECTIVITY_HIGH);
    }

    const auto row_count = Cardinality{input_table_statistics->row_count * PLACEHOLDER_SELECTIVITY_HIGH};
    return std::make_shared<TableStatistics>(std::move(output_column_statistics), row_count);
  }

  if (const auto in_expression = std::dynamic_pointer_cast<InExpression>(predicate)) {
    // Estimate `x IN (1, 2, 3)` by treating it as `x = 1 OR x = 2 ...`
    if (in_expression->set()->type != ExpressionType::List) {
      // Cannot handle subqueries
      return input_table_statistics;
    }

    const auto& list_expression = static_cast<const ListExpression&>(*in_expression->set());
    auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
    expressions.reserve(list_expression.elements().size());
    for (const auto& list_element : list_expression.elements()) {
      expressions.emplace_back(equals_(in_expression->operand(), list_element));
    }

    const auto disjunction = inflate_logical_expressions(expressions, LogicalOperator::Or);
    const auto new_predicate_node = PredicateNode::make(disjunction, predicate_node.left_input());
    return estimate_predicate_node(*new_predicate_node, input_table_statistics, cacheable, statistics_cache);
  }

  const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate, predicate_node);

  // TODO(anybody): Complex predicates are not processed right now and statistics objects are forwarded.
  //               That implies estimating a selectivity of 1 for such predicates.
  if (!operator_scan_predicates) {
    // We can obtain predicates with subquery results from the JoinToPredicateRewriteRule, which turns (semi-)joins
    // into predicates. OperatorScanPredicate::from_expression(...) cannot resolve these predicates. They act as a
    // filter comparable to a semi-join with the join key of the subquery result (see examples below).
    //
    // The JoinToPredicateRewriteRule checks that all preconditions are met to ensure correct query results.
    // Especially, it guarantees that the subqueries return a single row. Thus, we do not check this here (also, the
    // TableScan operator checks this during execution). For more information about this query rewrite, see
    // `join_to_predicate_rewrite_rule.hpp`. In the following, we only check if the predicates look the way they should
    // after the mentioned optimizer rule has reformulated them. If this is the case, we estimate their cardinality in
    // the same way we do for the original, not rewritten semi-joins. In case other subquery predicates are found which
    // have a different structure as expected, we default to assume the worst case: the input is not filtered at all
    // and we return the input statistics.
    //
    // The JoinToPredicateRewriteRule creates query plans that look loke this:
    //
    // Case (i): An equals predicate on a unique column guarantees to emit a single tuple, where we scan another table
    // for the resulting join key:
    //
    //                 [ Predicate n_regionkey = <subquery> ]
    //                 /                             |
    //                /                  [ Projection r_regionkey ]
    //               |                               |
    //               |                  [ Predicate r_name = 'ASIA' ]
    //               |                               |
    //   [ StoredTableNode nation ]      [ StoredTableNode region ]
    //
    // Case (ii): A between predicate on a column with an order dependency (OD) on the join key guarantees to emit the
    // minimal and maximal join key. In the example, the OD d_date_sk |-> d_year holds, i.e., ordering date_dim by
    // d_date_sk also orders d_year. Thus, a tuple with a smaller d_year than another tuple also has a smaller
    // d_date_sk (d_date_sk for d_year = 2001 is always smaller than for d_year = 2001). Selecting a year or a
    // sequence of years (d_year = 2000 or d_year BETWEEN 2000 AND 2001) guarantees all join keys between the min and
    // max join key appear in the selected tuple. We scan the other web_sales by these min/max values rather than
    // joining the two tables.
    //
    //              [ Predicate ws_sold_date_sk BETWEEN <subquery_a> AND <subquery_b> ]
    //                /                                     |                |
    //               |                  [ Projection MIN(d_date_sk ) ]  [ Projection MAX(d_date_sk) ]
    //               |                                      |                |
    //               |                          [ Aggregate MIN(d_date_sk), MAX(d_date_sk) ]
    //               |                                             |
    //               |                                  [ Predicate d_year = 2000 ]
    //               |                                             |
    //   [ StoredTableNode web_sales ]                  [ StoredTableNode date_dim ]
    //
    const auto predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate);
    if (!predicate_expression) {
      return input_table_statistics;
    }

    const auto& arguments = predicate_expression->arguments;
    auto subquery_statistics = std::shared_ptr<TableStatistics>{};
    auto subquery_column_id = ColumnID{0};
    auto column_expression = std::shared_ptr<AbstractExpression>{};
    const auto predicate_condition = predicate_expression->predicate_condition;

    // Case (i): Binary predicate with column = <subquery>. Equivalent to a semi-join with a table containing one row.
    // The assumption is that a predicate before filters for one tuple from a unique column. This was checked by the
    // JoinToPredicateRewriteRule.
    // Example query:
    //     SELECT n_name FROM nation WHERE n_regionkey = (SELECT r_regionkey FROM region WHERE r_name = 'ASIA');
    // We can get the statistics directly from the LQPSubqueryExpression.
    if (predicate_condition == PredicateCondition::Equals) {
      // Predicate: <subquery> = column.
      auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(arguments[0]);
      column_expression = arguments[1];

      // Predicate: column = <subquery>.
      if (!subquery_expression) {
        subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(arguments[1]);
        column_expression = arguments[0];
      }

      // Break if the predicate has no subquery as argument or the subquery is correlated. For instance, a predicate
      // only with value expressions (e.g., 1 = 2).
      if (!subquery_expression || subquery_expression->is_correlated()) {
        return input_table_statistics;
      }

      subquery_statistics = estimate_statistics(subquery_expression->lqp, cacheable, statistics_cache);
    }

    // Case (ii): Between predicate with column BETWEEN min(<subquery) AND max(<subquery>). Equivalent to a semi-join
    // with the referenced table, where the min/max aggregates of the join key select the range of matching keys.
    // Example query:
    //     SELECT SUM(ws_ext_sales_price) FROM web_sales
    //      WHERE ws_sold_date_sk BETWEEN (SELECT MIN(d_date_sk) FROM date_dim WHERE d_year = 2000)
    //                                AND (SELECT MAX(d_date_sk) FROM date_dim WHERE d_year = 2000);
    // However, we must ensure that we have a min/max aggregate to get the the lower/upper bound of the join key.
    if (is_between_predicate_condition(predicate_condition)) {
      column_expression = arguments[0];
      const auto& lower_bound_subquery = std::dynamic_pointer_cast<LQPSubqueryExpression>(arguments[1]);
      const auto& upper_bound_subquery = std::dynamic_pointer_cast<LQPSubqueryExpression>(arguments[2]);
      if (!lower_bound_subquery || !upper_bound_subquery || lower_bound_subquery->is_correlated() ||
          upper_bound_subquery->is_correlated()) {
        return input_table_statistics;
      }

      // Check that input nodes provide only a single WindowFunctionExpression.
      const auto& lower_bound_lqp = *lower_bound_subquery->lqp;
      const auto& upper_bound_lqp = *upper_bound_subquery->lqp;
      const auto& lower_bound_node_expressions = lower_bound_lqp.node_expressions;
      const auto& upper_bound_node_expressions = upper_bound_lqp.node_expressions;
      if (lower_bound_node_expressions.size() != 1 || upper_bound_node_expressions.size() != 1) {
        return input_table_statistics;
      }
      const auto& lower_bound_aggregate_expression =
          std::dynamic_pointer_cast<WindowFunctionExpression>(lower_bound_node_expressions.front());
      const auto& upper_bound_aggregate_expression =
          std::dynamic_pointer_cast<WindowFunctionExpression>(upper_bound_node_expressions.front());
      if (!lower_bound_aggregate_expression || !upper_bound_aggregate_expression) {
        return input_table_statistics;
      }

      // Check that the WindowFunctions are as expected and are performed on the same column, and the nodes have the
      // same input. The predicate must look like `BETWEEN (SELECT MIN(key) ...) AND (SELECT MAX(key) ...))`. The
      // aggregates guarantee to select the minimal and maximal join key of the underlying subquery. Furthermore, they
      // must both operate on the same join key and on the same input so preserve all join keys. A side effect of
      // enforcing the same input node is that the aggregates must stem from the same subquery, which is only possible
      // from a query rewrite. If they stem from a user's query, there would be two subqueries (whose operators will be
      // de-duplicated in the LQPTranslator).
      auto subquery_origin_node = lower_bound_lqp.left_input();

      if (lower_bound_aggregate_expression->window_function != WindowFunction::Min ||
          upper_bound_aggregate_expression->window_function != WindowFunction::Max ||
          *lower_bound_aggregate_expression->argument() != *upper_bound_aggregate_expression->argument() ||
          *subquery_origin_node != *upper_bound_lqp.left_input()) {
        return input_table_statistics;
      }

      // The lower/upper bound nodes might not be AggregateNodes themselves, but projections on a common AggregateNode.
      // In this case, check that (i) their input is an AggregateNode and (ii) it aggregates the min/max of the common
      // column.
      const auto lower_bound_type = lower_bound_lqp.type;
      const auto upper_bound_type = upper_bound_lqp.type;
      if ((lower_bound_type != LQPNodeType::Aggregate || upper_bound_type != LQPNodeType::Aggregate) &&
          (lower_bound_type != LQPNodeType::Projection || upper_bound_type != LQPNodeType::Projection ||
           subquery_origin_node->type != LQPNodeType::Aggregate)) {
        return input_table_statistics;
      }

      // If the aggregation of the join key is performed by a single AggregateNode, it must only aggregate the min
      // and max of the join key.
      if (subquery_origin_node->type == LQPNodeType::Aggregate) {
        const auto& node_expressions = subquery_origin_node->node_expressions;
        // Check that the AggregateNode only aggregates the min and max join key. By checking the number of node
        // expressions, we also ensure the values are not grouped by any column: If the AggregateNode has two node
        // expressions, one is the MIN(...) and one is the MAX(...), there cannot be another node expression for a
        // GROUP BY column.
        if (node_expressions.size() != 2 || !find_expression_idx(*lower_bound_aggregate_expression, node_expressions) ||
            !find_expression_idx(*upper_bound_aggregate_expression, node_expressions)) {
          return input_table_statistics;
        }

        subquery_origin_node = subquery_origin_node->left_input();
      }

      subquery_statistics = estimate_statistics(subquery_origin_node, cacheable, statistics_cache);
      subquery_column_id = subquery_origin_node->get_column_id(*lower_bound_aggregate_expression->argument());
    }

    if (!subquery_statistics) {
      return input_table_statistics;
    }

    // We do not have to further check if the subqueries return at most one row. This will be ensured during execution
    // by the TableScan operator.
    const auto column_id = predicate_node.left_input()->get_column_id(*column_expression);
    return estimate_semi_join(column_id, subquery_column_id, *input_table_statistics, *subquery_statistics);
  }

  // Scale the input statistics consequently for each predicate, assuming there are no correlations between them.
  auto output_table_statistics = input_table_statistics;
  for (const auto& operator_scan_predicate : *operator_scan_predicates) {
    if constexpr (HYRISE_DEBUG) {
      assert_required_statistics(operator_scan_predicate.column_id, predicate_node.left_input(),
                                 input_table_statistics);
      if (is_column_id(operator_scan_predicate.value)) {
        const auto column_id = boost::get<ColumnID>(operator_scan_predicate.value);
        assert_required_statistics(column_id, predicate_node.left_input(), input_table_statistics);
      }
    }
    output_table_statistics = estimate_operator_scan_predicate(output_table_statistics, operator_scan_predicate);
  }

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_join_node(
    const JoinNode& join_node, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics>& right_input_table_statistics) {
  // For inner-equi JoinNodes, a principle-of-inclusion algorithm is used.
  // The same algorithm is used for outer-equi JoinNodes, lacking a better alternative at the moment.
  // All other join modes and predicate conditions are treated as cross joins for now.

  if (join_node.join_mode == JoinMode::Cross) {
    return estimate_cross_join(*left_input_table_statistics, *right_input_table_statistics);
  }

  // TODO(anybody): Join cardinality estimation is consciously only performed for the primary join predicate, see #1560.
  const auto primary_operator_join_predicate = OperatorJoinPredicate::from_expression(
      *join_node.join_predicates()[0], *join_node.left_input(), *join_node.right_input());

  if (primary_operator_join_predicate) {
    if constexpr (HYRISE_DEBUG) {
      assert_required_statistics(primary_operator_join_predicate->column_ids.first, join_node.left_input(),
                                 left_input_table_statistics);
      assert_required_statistics(primary_operator_join_predicate->column_ids.second, join_node.right_input(),
                                 right_input_table_statistics);
    }

    switch (join_node.join_mode) {
      // For now, handle outer joins just as inner joins.
      // TODO(anybody): Handle them more accurately, i.e., estimate how many tuples do not find matches, see #1830.
      case JoinMode::Left:
      case JoinMode::Right:
      case JoinMode::FullOuter:
      case JoinMode::Inner:
        switch (primary_operator_join_predicate->predicate_condition) {
          case PredicateCondition::Equals:
            return estimate_inner_equi_join(primary_operator_join_predicate->column_ids.first,
                                            primary_operator_join_predicate->column_ids.second,
                                            *left_input_table_statistics, *right_input_table_statistics);

          // TODO(anybody): Implement estimation for non-equi joins, see #1830.
          case PredicateCondition::NotEquals:
          case PredicateCondition::LessThan:
          case PredicateCondition::LessThanEquals:
          case PredicateCondition::GreaterThan:
          case PredicateCondition::GreaterThanEquals:
          case PredicateCondition::BetweenInclusive:
          case PredicateCondition::BetweenUpperExclusive:
          case PredicateCondition::BetweenLowerExclusive:
          case PredicateCondition::BetweenExclusive:
          case PredicateCondition::In:
          case PredicateCondition::NotIn:
          case PredicateCondition::Like:
          case PredicateCondition::NotLike:
            return estimate_cross_join(*left_input_table_statistics, *right_input_table_statistics);

          case PredicateCondition::IsNull:
          case PredicateCondition::IsNotNull:
            Fail("IS NULL is an invalid join predicate");
        }
        Fail("Invalid enum value.");

      case JoinMode::Cross:
        // Should have been forwarded to estimate_cross_join()
        Fail("Cross join is not a predicated join");

      case JoinMode::Semi:
        return estimate_semi_join(primary_operator_join_predicate->column_ids.first,
                                  primary_operator_join_predicate->column_ids.second, *left_input_table_statistics,
                                  *right_input_table_statistics);

      case JoinMode::AntiNullAsTrue:
      case JoinMode::AntiNullAsFalse:
        return left_input_table_statistics;
    }
  }

  // TODO(anybody): For now, estimate a selectivity of one, see #1830.
  return estimate_cross_join(*left_input_table_statistics, *right_input_table_statistics);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_union_node(
    const UnionNode& /*union_node*/, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics>& right_input_table_statistics) {
  // Since UnionNodes are not really used right now, implementing an involved algorithm to union two TableStatistics
  // seems unjustified. For now, we just concatenate the two statistics objects

  DebugAssert(
      left_input_table_statistics->column_statistics.size() == right_input_table_statistics->column_statistics.size(),
      "Input TableStatistics need to have the same number of columns to perform a union");

  auto column_statistics = left_input_table_statistics->column_statistics;

  const auto row_count = Cardinality{left_input_table_statistics->row_count + right_input_table_statistics->row_count};

  auto output_table_statistics = std::make_shared<TableStatistics>(std::move(column_statistics), row_count);

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_limit_node(
    const LimitNode& limit_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For LimitNodes with a value as limit_expression, create a TableStatistics object with that value as row_count.
  // Otherwise, forward the input statistics for now.

  if (const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(limit_node.num_rows_expression())) {
    const auto row_count = lossy_variant_cast<Cardinality>(value_expression->value);
    if (!row_count) {
      // `value_expression->value` being NULL does not make much sense, but that is not the concern of the
      // CardinalityEstimator.
      return input_table_statistics;
    }

    // Number of rows can never exceed number of input rows.
    const auto clamped_row_count = std::min(*row_count, input_table_statistics->row_count);

    auto column_statistics =
        std::vector<std::shared_ptr<const BaseAttributeStatistics>>{limit_node.output_expressions().size()};

    for (auto column_id = ColumnID{0}; column_id < input_table_statistics->column_statistics.size(); ++column_id) {
      column_statistics[column_id] =
          std::make_shared<DummyStatistics>(input_table_statistics->column_data_type(column_id));
    }

    return std::make_shared<TableStatistics>(std::move(column_statistics), clamped_row_count);
  }

  return input_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_operator_scan_predicate(
    const std::shared_ptr<TableStatistics>& input_table_statistics, const OperatorScanPredicate& predicate) {
  /**
   * This function analyses the `predicate` and dispatches an appropriate selectivity-estimating algorithm.
   */

  auto selectivity = Selectivity{1};

  const auto left_column_id = predicate.column_id;
  auto right_column_id = std::optional<ColumnID>{};

  const auto left_input_base_column_statistics = input_table_statistics->column_statistics[left_column_id];
  const auto left_data_type = input_table_statistics->column_data_type(left_column_id);

  auto output_column_statistics =
      std::vector<std::shared_ptr<const BaseAttributeStatistics>>{input_table_statistics->column_statistics.size()};

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto left_input_column_statistics =
        std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(left_input_base_column_statistics);
    if (!left_input_column_statistics) {
      selectivity = PLACEHOLDER_SELECTIVITY_ALL;
      return;
    }

    /**
     * Estimate IS (NOT) NULL
     */
    if (predicate.predicate_condition == PredicateCondition::IsNull ||
        predicate.predicate_condition == PredicateCondition::IsNotNull) {
      const auto is_not_null = predicate.predicate_condition == PredicateCondition::IsNotNull;

      const auto null_value_ratio =
          estimate_null_value_ratio_of_column(*input_table_statistics, *left_input_column_statistics);

      if (null_value_ratio) {
        selectivity = is_not_null ? 1 - *null_value_ratio : *null_value_ratio;

        // All that remains of the column we scanned on are exclusively NULL values or exclusively non-NULL values.
        const auto column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
        column_statistics->null_value_ratio = std::make_shared<NullValueRatioStatistics>(is_not_null ? 0.0 : 1.0);
        if (is_not_null) {
          // Forward other statistics if NULLs are removed.
          const auto& input_statistics = *left_input_column_statistics;
          column_statistics->histogram = input_statistics.histogram;
          column_statistics->min_max_filter = input_statistics.min_max_filter;
          column_statistics->range_filter = input_statistics.range_filter;
          column_statistics->distinct_value_count = input_statistics.distinct_value_count;
        }
        output_column_statistics[left_column_id] = column_statistics;
      } else {
        // If there is no null-value ratio available, assume a selectivity of 1, for both IS NULL and IS NOT NULL, as no
        // magic number makes real sense here.
        selectivity = PLACEHOLDER_SELECTIVITY_ALL;
        return;
      }
    } else {
      const auto scan_statistics_object = left_input_column_statistics->histogram;
      // If there are no statistics available for this segment, assume a selectivity of 1, as no magic number makes real
      // sense here.
      if (!scan_statistics_object) {
        selectivity = PLACEHOLDER_SELECTIVITY_ALL;
        return;
      }

      /**
       * Estimate ColumnVsColumn
       */
      if (predicate.value.type() == typeid(ColumnID)) {
        right_column_id = boost::get<ColumnID>(predicate.value);

        const auto right_data_type = input_table_statistics->column_data_type(*right_column_id);

        if (left_data_type != right_data_type || left_data_type == DataType::String) {
          // TODO(anyone): Cannot estimate column-vs-column scan for differing data types, yet. Furthermore,
          //               `split_at_bin_bounds() is not yet supported for strings and we cannot properly estimate
          //               string comparisons, either.
          selectivity = PLACEHOLDER_SELECTIVITY_ALL;
          return;
        }

        if (predicate.predicate_condition != PredicateCondition::Equals) {
          // TODO(anyone): CardinalityEstimator cannot handle non-equi column-to-column scans right now.
          selectivity = PLACEHOLDER_SELECTIVITY_ALL;
          return;
        }

        const auto right_input_column_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(
            input_table_statistics->column_statistics[*right_column_id]);

        const auto left_histogram = left_input_column_statistics->histogram;
        const auto right_histogram = right_input_column_statistics->histogram;
        if (!left_histogram || !right_histogram) {
          // Can only use histograms to estimate column-to-column scans right now.
          // TODO(anyone): extend to other statistics objects.
          selectivity = PLACEHOLDER_SELECTIVITY_ALL;
          return;
        }

        const auto bin_adjusted_left_histogram = left_histogram->split_at_bin_bounds(right_histogram->bin_bounds());
        const auto bin_adjusted_right_histogram = right_histogram->split_at_bin_bounds(left_histogram->bin_bounds());

        const auto column_vs_column_histogram = estimate_column_vs_column_equi_scan_with_histograms(
            *bin_adjusted_left_histogram, *bin_adjusted_right_histogram);
        if (!column_vs_column_histogram) {
          // No overlapping bins: No rows selected
          selectivity = 0.0;
          return;
        }

        const auto cardinality = column_vs_column_histogram->total_count();
        selectivity = input_table_statistics->row_count == 0 ? 0.0 : cardinality / input_table_statistics->row_count;

        /**
         * Write out the AttributeStatistics of the scanned columns
         */
        const auto column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
        column_statistics->histogram = column_vs_column_histogram;
        output_column_statistics[left_column_id] = column_statistics;
        output_column_statistics[*right_column_id] = column_statistics;

      } else if (predicate.value.type() == typeid(ParameterID)) {
        /**
         * Estimate ColumnVsPlaceholder
         */

        switch (predicate.predicate_condition) {
          case PredicateCondition::Equals: {
            const auto total_distinct_count =
                std::max(scan_statistics_object->total_distinct_count(), HistogramCountType{1.0});
            selectivity = total_distinct_count > 0 ? 1.0 / total_distinct_count : 0.0;
          } break;

          case PredicateCondition::NotEquals: {
            const auto total_distinct_count =
                std::max(scan_statistics_object->total_distinct_count(), HistogramCountType{1.0});
            selectivity = total_distinct_count > 0 ? (total_distinct_count - 1.0) / total_distinct_count : 0.0;
          } break;

          case PredicateCondition::LessThan:
          case PredicateCondition::LessThanEquals:
          case PredicateCondition::GreaterThan:
          case PredicateCondition::GreaterThanEquals:
          case PredicateCondition::BetweenInclusive:
          case PredicateCondition::BetweenExclusive:
          case PredicateCondition::BetweenLowerExclusive:
          case PredicateCondition::BetweenUpperExclusive:
          case PredicateCondition::In:
          case PredicateCondition::NotIn:
          case PredicateCondition::Like:
          case PredicateCondition::NotLike:
            // Lacking better options, assume a "magic" selectivity for >, >=, <, <=, ... Any number would be equally
            // right and wrong here. In some examples, this seemed like a good guess ¯\_(ツ)_/¯
            selectivity = PLACEHOLDER_SELECTIVITY_MEDIUM;
            break;

          case PredicateCondition::IsNull:
          case PredicateCondition::IsNotNull:
            Fail("IS (NOT) NULL predicates should not have a 'value' parameter.");
        }

      } else {
        /**
         * Estimate ColumnVsValue / ColumnBetween
         */
        Assert(predicate.value.type() == typeid(AllTypeVariant), "Expected AllTypeVariant");

        const auto value_variant = boost::get<AllTypeVariant>(predicate.value);
        if (variant_is_null(value_variant)) {
          // A predicate `<column> <condition> NULL` always has a selectivity of 0
          selectivity = 0.0;
          return;
        }

        if (predicate.predicate_condition == PredicateCondition::Like) {
          // Lacking better options, assume a "magic" selectivity for LIKE. Any number would be equally
          // right and wrong here. In some examples, this seemed like a good guess ¯\_(ツ)_/¯
          selectivity = PLACEHOLDER_SELECTIVITY_LOW;
          return;
        }
        if (predicate.predicate_condition == PredicateCondition::NotLike) {
          // Lacking better options, assume a "magic" selectivity for NOT LIKE. Any number would be equally
          // right and wrong here. In some examples, this seemed like a good guess ¯\_(ツ)_/¯
          selectivity = PLACEHOLDER_SELECTIVITY_HIGH;
          return;
        }

        auto value2_variant = std::optional<AllTypeVariant>{};
        if (predicate.value2) {
          if (predicate.value2->type() != typeid(AllTypeVariant)) {
            // Lacking better options, assume a "magic" selectivity for `BETWEEN ... AND ?`. Any number would be equally
            // right and wrong here. In some examples, this seemed like a good guess ¯\_(ツ)_/¯
            selectivity = PLACEHOLDER_SELECTIVITY_MEDIUM;
            return;
          }

          value2_variant = boost::get<AllTypeVariant>(*predicate.value2);
        }

        const auto sliced_statistics_object =
            scan_statistics_object->sliced(predicate.predicate_condition, value_variant, value2_variant);
        if (!sliced_statistics_object) {
          selectivity = 0.0;
          return;
        }

        // TODO(anyone): Simplify this block if AbstractStatisticsObject ever supports `total_count()`.
        const auto sliced_histogram =
            std::dynamic_pointer_cast<const AbstractHistogram<ColumnDataType>>(sliced_statistics_object);
        DebugAssert(sliced_histogram, "Expected slicing of a Histogram to return either nullptr or a Histogram");
        if (input_table_statistics->row_count == 0 || sliced_histogram->total_count() == 0.0) {
          selectivity = 0.0;
        } else {
          selectivity = sliced_histogram->total_count() / input_table_statistics->row_count;
        }
        const auto column_statistics =
            left_input_column_statistics->sliced(predicate.predicate_condition, value_variant, value2_variant);
        output_column_statistics[left_column_id] = column_statistics;
      }
    }
  });

  // Entire chunk matches: simply return the input.
  if (selectivity == Selectivity{1}) {
    return input_table_statistics;
  }

  // Scale the other columns' AttributeStatistics (those that we didn't write to above) with the selectivity
  for (auto column_id = ColumnID{0}; column_id < output_column_statistics.size(); ++column_id) {
    if (!output_column_statistics[column_id]) {
      output_column_statistics[column_id] = input_table_statistics->column_statistics[column_id]->scaled(selectivity);
    }
  }

  const auto row_count = Cardinality{input_table_statistics->row_count * selectivity};
  return std::make_shared<TableStatistics>(std::move(output_column_statistics), row_count);
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> CardinalityEstimator::estimate_column_vs_column_equi_scan_with_histograms(
    const AbstractHistogram<T>& left_histogram, const AbstractHistogram<T>& right_histogram) {
  /**
   * Column-to-column scan estimation is notoriously hard; selectivities from 0 to 1 are possible for the same
   * histogram pairs. Thus, we do the most conservative estimation and compute the upper bound of value- and distinct
   * counts for each bin pair. This means that all values that could have a match, i.e., values that are in both
   * histograms, survive.
   */
  auto left_idx = BinID{0};
  auto right_idx = BinID{0};
  auto left_bin_count = left_histogram.bin_count();
  auto right_bin_count = right_histogram.bin_count();

  auto builder = GenericHistogramBuilder<T>{};

  while (left_idx < left_bin_count && right_idx < right_bin_count) {
    const auto& left_min = left_histogram.bin_minimum(left_idx);
    const auto& right_min = right_histogram.bin_minimum(right_idx);

    // We split both histograms at each others' bounds before. Thus, we can skip bins that have different bounds (i.e.,
    // bins that exist in only one of the two histograms) because their values cannot have matches in the other
    // histogram.
    if (left_min < right_min) {
      ++left_idx;
      continue;
    }

    if (right_min < left_min) {
      ++right_idx;
      continue;
    }

    DebugAssert(left_histogram.bin_maximum(left_idx) == right_histogram.bin_maximum(right_idx),
                "Histogram bin boundaries do not match.");

    // Assume that all values that could have a match survive. Values that are only in one histogram cannot match. Thus,
    // we use the minima of height and distinct count from the input histograms for the new histogram.
    const auto height = std::min(left_histogram.bin_height(left_idx), right_histogram.bin_height(right_idx));
    const auto distinct_count =
        std::min(left_histogram.bin_distinct_count(left_idx), right_histogram.bin_distinct_count(right_idx));

    if (height > 0 && distinct_count > 0) {
      builder.add_bin(left_min, left_histogram.bin_maximum(left_idx), height, distinct_count);
    }

    ++left_idx;
    ++right_idx;
  }

  if (builder.empty()) {
    return nullptr;
  }

  return builder.build();
}

template std::shared_ptr<GenericHistogram<int32_t>>
CardinalityEstimator::estimate_column_vs_column_equi_scan_with_histograms(
    const AbstractHistogram<int32_t>& left_histogram, const AbstractHistogram<int32_t>& right_histogram);
template std::shared_ptr<GenericHistogram<int64_t>>
CardinalityEstimator::estimate_column_vs_column_equi_scan_with_histograms(
    const AbstractHistogram<int64_t>& left_histogram, const AbstractHistogram<int64_t>& right_histogram);
template std::shared_ptr<GenericHistogram<float>>
CardinalityEstimator::estimate_column_vs_column_equi_scan_with_histograms(
    const AbstractHistogram<float>& left_histogram, const AbstractHistogram<float>& right_histogram);
template std::shared_ptr<GenericHistogram<double>>
CardinalityEstimator::estimate_column_vs_column_equi_scan_with_histograms(
    const AbstractHistogram<double>& left_histogram, const AbstractHistogram<double>& right_histogram);
template std::shared_ptr<GenericHistogram<pmr_string>>
CardinalityEstimator::estimate_column_vs_column_equi_scan_with_histograms(
    const AbstractHistogram<pmr_string>& left_histogram, const AbstractHistogram<pmr_string>& right_histogram);

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_inner_equi_join(
    const ColumnID left_column_id, const ColumnID right_column_id, const TableStatistics& left_input_table_statistics,
    const TableStatistics& right_input_table_statistics) {
  const auto left_data_type = left_input_table_statistics.column_data_type(left_column_id);
  const auto right_data_type = right_input_table_statistics.column_data_type(right_column_id);

  // We expect both columns to be of the same type. This allows us to resolve the type only once, reducing the
  // compile time. For differing column types and/or string columns (which we cannot handle right now), we assume that
  // all tuples qualify. This is probably a gross overestimation, but we need to return something...
  // TODO(anyone): - Implement join estimation for differing column data types.
  //               - Implement join estimation for String columns.
  if (left_data_type != right_data_type || left_data_type == DataType::String) {
    return estimate_cross_join(left_input_table_statistics, right_input_table_statistics);
  }

  auto output_table_statistics = std::shared_ptr<TableStatistics>{};

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto left_input_column_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(
        left_input_table_statistics.column_statistics[left_column_id]);
    const auto right_input_column_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(
        right_input_table_statistics.column_statistics[right_column_id]);

    auto cardinality = Cardinality{0};
    auto join_column_histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

    auto left_histogram = left_input_column_statistics ? left_input_column_statistics->histogram : nullptr;
    auto right_histogram = right_input_column_statistics ? right_input_column_statistics->histogram : nullptr;

    if (left_histogram && right_histogram) {
      // If we have histograms, we use the principle of inclusion to determine the number of matches between two bins.
      join_column_histogram = estimate_inner_equi_join_with_histograms(*left_histogram, *right_histogram);
      cardinality = join_column_histogram->total_count();
    } else {
      // TODO(anyone): If we do hot have histograms on both sides, use some other algorithm/statistics to estimate the
      //               join.
      const auto left_cardinality = left_input_table_statistics.row_count;
      const auto right_cardinality = right_input_table_statistics.row_count;
      cardinality = std::max({left_cardinality, right_cardinality, left_cardinality * right_cardinality});
    }

    const auto left_selectivity = Selectivity{
        left_input_table_statistics.row_count > 0 ? cardinality / left_input_table_statistics.row_count : 0.0};
    const auto right_selectivity = Selectivity{
        right_input_table_statistics.row_count > 0 ? cardinality / right_input_table_statistics.row_count : 0.0};

    /**
     * Write out the AttributeStatistics of all output columns. With no correlation info available, simply scale all
     * those that didn't participate in the join predicate
     */
    const auto left_column_count = left_input_table_statistics.column_statistics.size();
    const auto right_column_count = right_input_table_statistics.column_statistics.size();
    auto column_statistics =
        std::vector<std::shared_ptr<const BaseAttributeStatistics>>(left_column_count + right_column_count);

    const auto join_columns_output_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
    join_columns_output_statistics->histogram = join_column_histogram;
    column_statistics[left_column_id] = join_columns_output_statistics;
    column_statistics[left_column_count + right_column_id] = join_columns_output_statistics;

    for (auto column_id = ColumnID{0}; column_id < left_column_count; ++column_id) {
      if (column_statistics[column_id]) {
        continue;
      }

      column_statistics[column_id] = left_input_table_statistics.column_statistics[column_id]->scaled(left_selectivity);
    }
    for (auto column_id = ColumnID{0}; column_id < right_column_count; ++column_id) {
      if (column_statistics[left_column_count + column_id]) {
        continue;
      }

      column_statistics[left_column_count + column_id] =
          right_input_table_statistics.column_statistics[column_id]->scaled(right_selectivity);
    }

    output_table_statistics = std::make_shared<TableStatistics>(std::move(column_statistics), cardinality);
  });

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_semi_join(
    const ColumnID left_column_id, const ColumnID right_column_id, const TableStatistics& left_input_table_statistics,
    const TableStatistics& right_input_table_statistics) {
  // This is based on estimate_inner_equi_join. We take the histogram from the right, set the bin heights to the
  // distinct counts and run an inner/equi estimation on it. As there are no more duplicates on the right side, we
  // should get the correct estimation for the left side.
  const auto left_data_type = left_input_table_statistics.column_data_type(left_column_id);
  const auto right_data_type = right_input_table_statistics.column_data_type(right_column_id);

  // We expect both columns to be of the same type. This allows us to resolve the type only once, reducing the
  // compile time. For differing column types and/or string columns (which we cannot handle right now), we assume that
  // all tuples qualify. This is probably a gross overestimation, but we need to return something...
  // TODO(anyone): - Implement join estimation for differing column data types.
  //               - Implement join estimation for string columns.
  if (left_data_type != right_data_type || left_data_type == DataType::String) {
    return std::make_shared<TableStatistics>(left_input_table_statistics);
  }

  auto output_table_statistics = std::shared_ptr<TableStatistics>{};

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto left_input_column_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(
        left_input_table_statistics.column_statistics[left_column_id]);
    const auto right_input_column_statistics = std::dynamic_pointer_cast<const AttributeStatistics<ColumnDataType>>(
        right_input_table_statistics.column_statistics[right_column_id]);

    auto cardinality = Cardinality{0};
    auto join_column_histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

    auto left_histogram = left_input_column_statistics ? left_input_column_statistics->histogram : nullptr;
    auto right_histogram = right_input_column_statistics ? right_input_column_statistics->histogram : nullptr;

    if (left_histogram && right_histogram) {
      // Adapt the right histogram so that it only covers distinct values (i.e., replacing the bins' height with their
      // number of distinct counts)
      auto distinct_right_histogram_builder =
          GenericHistogramBuilder(right_histogram->bin_count(), right_histogram->domain());
      const auto right_bin_count = right_histogram->bin_count();
      for (auto bin_id = BinID{0}; bin_id < right_bin_count; ++bin_id) {
        const auto& right_bin = right_histogram->bin(bin_id);
        distinct_right_histogram_builder.add_bin(right_bin.min, right_bin.max, right_bin.distinct_count,
                                                 right_bin.distinct_count);
      }

      const auto distinct_right_histogram = distinct_right_histogram_builder.build();
      // If we have histograms, we use the principle of inclusion to determine the number of matches between two bins.
      join_column_histogram = estimate_inner_equi_join_with_histograms(*left_histogram, *distinct_right_histogram);
      cardinality = join_column_histogram->total_count();
    } else {
      // TODO(anyone): If we do not have histograms on both sides, use some other algorithm/statistics to estimate the
      //               join.
      cardinality = left_input_table_statistics.row_count;
    }

    const auto selectivity = Selectivity{
        left_input_table_statistics.row_count > 0 ? cardinality / left_input_table_statistics.row_count : 0.0};

    /**
     * Write out the AttributeStatistics of all output columns. With no correlation info available, simply scale all
     * those that didn't participate in the join predicate
     */
    const auto column_count = left_input_table_statistics.column_statistics.size();
    auto column_statistics = std::vector<std::shared_ptr<const BaseAttributeStatistics>>(column_count);

    const auto join_columns_output_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
    join_columns_output_statistics->histogram = join_column_histogram;
    column_statistics[left_column_id] = join_columns_output_statistics;

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      if (column_statistics[column_id]) {
        continue;
      }

      column_statistics[column_id] = left_input_table_statistics.column_statistics[column_id]->scaled(selectivity);
    }

    output_table_statistics = std::make_shared<TableStatistics>(std::move(column_statistics), cardinality);
  });

  Assert(output_table_statistics->row_count <= left_input_table_statistics.row_count * 1.01,
         "Semi join should not increase cardinality");

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_cross_join(
    const TableStatistics& left_input_table_statistics, const TableStatistics& right_input_table_statistics) {
  // Every tuple from the left side gets emitted once for each tuple on the right side - and vice versa.
  const auto left_selectivity = Selectivity{right_input_table_statistics.row_count};
  const auto right_selectivity = Selectivity{left_input_table_statistics.row_count};

  /**
   * Scale up the input AttributeStatistics with the selectivities specified above and write them to the output
   * TableStatistics.
   */
  const auto left_column_count = left_input_table_statistics.column_statistics.size();
  const auto right_column_count = right_input_table_statistics.column_statistics.size();
  auto column_statistics =
      std::vector<std::shared_ptr<const BaseAttributeStatistics>>(left_column_count + right_column_count);

  for (auto column_id = ColumnID{0}; column_id < left_column_count; ++column_id) {
    column_statistics[column_id] = left_input_table_statistics.column_statistics[column_id]->scaled(left_selectivity);
  }

  for (auto column_id = ColumnID{0}; column_id < right_column_count; ++column_id) {
    column_statistics[left_column_count + column_id] =
        right_input_table_statistics.column_statistics[column_id]->scaled(right_selectivity);
  }

  const auto row_count =
      Cardinality{std::max({left_selectivity, right_selectivity, left_selectivity * right_selectivity})};

  return std::make_shared<TableStatistics>(std::move(column_statistics), row_count);
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> CardinalityEstimator::estimate_inner_equi_join_with_histograms(
    const AbstractHistogram<T>& left_histogram, const AbstractHistogram<T>& right_histogram) {
  /**
   * left_histogram and right_histogram are turned into "unified" histograms by `split_at_bin_bounds`, meaning that
   * their bins are split so that their bin boundaries match.
   * E.g., if left_histogram has a single bin [1, 10] and right histogram has a single bin [5, 20] then
   * unified_left_histogram == {[1, 4], [5, 10]}
   * unified_right_histogram == {[5, 10], [11, 20]}
   * The estimation is performed on overlapping bins only, e.g., only the two bins [5, 10] will produce matches.
   */

  auto unified_left_histogram = left_histogram.split_at_bin_bounds(right_histogram.bin_bounds());
  auto unified_right_histogram = right_histogram.split_at_bin_bounds(left_histogram.bin_bounds());

  auto left_idx = BinID{0};
  auto right_idx = BinID{0};
  auto left_bin_count = unified_left_histogram->bin_count();
  auto right_bin_count = unified_right_histogram->bin_count();

  auto builder = GenericHistogramBuilder<T>{};

  // Iterate over both unified histograms and find overlapping bins.
  while (left_idx < left_bin_count && right_idx < right_bin_count) {
    const auto& left_min = unified_left_histogram->bin_minimum(left_idx);
    const auto& right_min = unified_right_histogram->bin_minimum(right_idx);

    if (left_min < right_min) {
      ++left_idx;
      continue;
    }

    if (right_min < left_min) {
      ++right_idx;
      continue;
    }

    DebugAssert(unified_left_histogram->bin_maximum(left_idx) == unified_right_histogram->bin_maximum(right_idx),
                "Histogram bin boundaries do not match.");

    // Overlapping bins found, estimate the join for these bins' range.
    const auto [height, distinct_count] = estimate_inner_equi_join_of_bins(
        unified_left_histogram->bin_height(left_idx), unified_left_histogram->bin_distinct_count(left_idx),
        unified_right_histogram->bin_height(right_idx), unified_right_histogram->bin_distinct_count(right_idx));

    if (height > 0) {
      builder.add_bin(left_min, unified_left_histogram->bin_maximum(left_idx), height, distinct_count);
    }

    ++left_idx;
    ++right_idx;
  }

  return builder.build();
}

template std::shared_ptr<GenericHistogram<int32_t>> CardinalityEstimator::estimate_inner_equi_join_with_histograms(
    const AbstractHistogram<int32_t>& left_histogram, const AbstractHistogram<int32_t>& right_histogram);
template std::shared_ptr<GenericHistogram<int64_t>> CardinalityEstimator::estimate_inner_equi_join_with_histograms(
    const AbstractHistogram<int64_t>& left_histogram, const AbstractHistogram<int64_t>& right_histogram);
template std::shared_ptr<GenericHistogram<float>> CardinalityEstimator::estimate_inner_equi_join_with_histograms(
    const AbstractHistogram<float>& left_histogram, const AbstractHistogram<float>& right_histogram);
template std::shared_ptr<GenericHistogram<double>> CardinalityEstimator::estimate_inner_equi_join_with_histograms(
    const AbstractHistogram<double>& left_histogram, const AbstractHistogram<double>& right_histogram);
template std::shared_ptr<GenericHistogram<pmr_string>> CardinalityEstimator::estimate_inner_equi_join_with_histograms(
    const AbstractHistogram<pmr_string>& left_histogram, const AbstractHistogram<pmr_string>& right_histogram);

std::pair<HistogramCountType, HistogramCountType> CardinalityEstimator::estimate_inner_equi_join_of_bins(
    const HistogramCountType left_height, const DistinctCount left_distinct_count,
    const HistogramCountType right_height, const DistinctCount right_distinct_count) {
  // Range with more distinct values should be on the left side to keep the algorithm below simple
  if (left_distinct_count < right_distinct_count) {
    return estimate_inner_equi_join_of_bins(right_height, right_distinct_count, left_height, left_distinct_count);
  }

  // Early out to avoid division by zero below
  if (left_distinct_count == 0 || right_distinct_count == 0) {
    return {HistogramCountType{0}, HistogramCountType{0}};
  }

  // Perform a basic principle-of-inclusion join estimation

  // Each distinct value on the right side is assumed to occur `right_density` times.
  // E.g., if right_height == 10 and right_distinct_count == 2, then each distinct value occurs 5 times.
  const auto right_density = right_height / right_distinct_count;

  // "principle-of-inclusion" means every distinct value on the right side finds a match left. `left_match_ratio` is the
  // ratio of distinct values on the left side that find a match on the right side
  // E.g., if right_distinct_count == 10 and left_distinct_count == 30, then one third of the rows from the
  // left side will find a match.
  const auto left_match_ratio = right_distinct_count / left_distinct_count;

  // `left_height * left_match_ratio` is the number of rows on the left side that will find matches. `right_density` is
  // the number of matches each row on the left side finds. Multiply them to get the number of resulting matches.
  const auto match_count = HistogramCountType{left_height * left_match_ratio * right_density};

  return {match_count, HistogramCountType{right_distinct_count}};
}

void CardinalityEstimator::_populate_required_column_expressions() const {
  // Do nothing if there is no plan referenced in the cache or statistics pruning is not turned on.
  if (!cardinality_estimation_cache.lqp || !cardinality_estimation_cache.required_column_expressions) {
    return;
  }

  auto node_queue = std::queue<std::shared_ptr<const AbstractLQPNode>>{};
  node_queue.push(cardinality_estimation_cache.lqp);
  auto visited_nodes = std::unordered_set<std::shared_ptr<const AbstractLQPNode>>{};

  while (!node_queue.empty()) {
    const auto node = node_queue.front();
    node_queue.pop();

    if (!visited_nodes.emplace(node).second) {
      continue;
    }

    if (node->type == LQPNodeType::Join || node->type == LQPNodeType::Predicate) {
      for (const auto& root_expression : node->node_expressions) {
        visit_expression(root_expression, [&](const auto& expression) {
          if (expression->type == ExpressionType::LQPColumn) {
            cardinality_estimation_cache.required_column_expressions->emplace(expression);
          } else if (expression->type == ExpressionType::LQPSubquery) {
            node_queue.push(static_cast<const LQPSubqueryExpression&>(*expression).lqp);
          }

          return ExpressionVisitation::VisitArguments;
        });
      }
    }

    if (node->left_input()) {
      node_queue.push(node->left_input());
    }
    if (node->right_input()) {
      node_queue.push(node->right_input());
    }
  }

  // Unset the plan so we do not populate the columns again if there are multiple base tables.
  cardinality_estimation_cache.lqp = nullptr;
}

}  // namespace hyrise
