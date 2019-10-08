#include "cardinality_estimator.hpp"

#include <iostream>
#include <memory>

#include "attribute_statistics.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "lossy_cast.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "resolve_type.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "statistics/statistics_objects/equal_distinct_count_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram_builder.hpp"
#include "storage/table.hpp"
#include "table_statistics.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT

// Magic constants used in places where a better estimation would be implementable (either with
// statistics objects not yet implemented or new algorithms) - but doing so just wasn't warranted yet.
constexpr auto PLACEHOLDER_SELECTIVITY_LOW = 0.1f;
constexpr auto PLACEHOLDER_SELECTIVITY_MEDIUM = 0.5f;
constexpr auto PLACEHOLDER_SELECTIVITY_HIGH = 0.9f;
constexpr auto PLACEHOLDER_SELECTIVITY_ALL = 1.0f;

template <typename T>
std::optional<float> estimate_null_value_ratio_of_column(const TableStatistics& table_statistics,
                                                         const AttributeStatistics<T>& column_statistics) {
  // If the column has an explicit null value ratio associated with it, we can just use that
  if (column_statistics.null_value_ratio) {
    return column_statistics.null_value_ratio->ratio;
  }

  // Otherwise derive the null value ratio from the total count of a histogram (which excludes NULLs) and the
  // TableStatistics row count (which includes NULLs)
  if (column_statistics.histogram && table_statistics.row_count != 0.0f) {
    return 1.0f - (static_cast<float>(column_statistics.histogram->total_count()) / table_statistics.row_count);
  }

  return std::nullopt;
}

}  // namespace

namespace opossum {

std::shared_ptr<AbstractCardinalityEstimator> CardinalityEstimator::new_instance() const {
  return std::make_shared<CardinalityEstimator>();
}

Cardinality CardinalityEstimator::estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  const auto estimated_statistics = estimate_statistics(lqp);
  return estimated_statistics->row_count;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_statistics(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  /**
   * 1. Try a cache lookup for requested LQP.
   *
   * The `join_graph_bitmask` is kept so that if cache lookup fails, a new cache entry with this bitmask as a key can
   * be created at the end of this function.
   *
   * Lookup in `join_graph_statistics_cache` is expected to have a higher hit rate (since every bitmask represents
   * multiple LQPs) than `statistics_by_lqp`. Thus lookup in `join_graph_statistics_cache` is performed first.
   */
  auto join_graph_bitmask = std::optional<JoinGraphStatisticsCache::Bitmask>{};
  if (cardinality_estimation_cache.join_graph_statistics_cache) {
    join_graph_bitmask = cardinality_estimation_cache.join_graph_statistics_cache->bitmask(lqp);
    if (join_graph_bitmask) {
      const auto cached_statistics =
          cardinality_estimation_cache.join_graph_statistics_cache->get(*join_graph_bitmask, lqp->column_expressions());
      if (cached_statistics) {
        return cached_statistics;
      }
    } else {
      // The LQP does not represent (a subgraph of) of a JoinGraph and therefore we cannot use the
      // cardinality_estimation_cache.join_graph_statistics_cache
    }
  }

  if (cardinality_estimation_cache.statistics_by_lqp) {
    const auto plan_statistics_iter = cardinality_estimation_cache.statistics_by_lqp->find(lqp);
    if (plan_statistics_iter != cardinality_estimation_cache.statistics_by_lqp->end()) {
      return plan_statistics_iter->second;
    }
  }

  /**
   * 2. Cache lookup failed - perform an actual cardinality estimation
   */
  auto output_table_statistics = std::shared_ptr<TableStatistics>{};
  const auto left_input_table_statistics = lqp->left_input() ? estimate_statistics(lqp->left_input()) : nullptr;
  const auto right_input_table_statistics = lqp->right_input() ? estimate_statistics(lqp->right_input()) : nullptr;

  switch (lqp->type) {
    case LQPNodeType::Aggregate: {
      const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(lqp);
      output_table_statistics = estimate_aggregate_node(*aggregate_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Alias: {
      const auto alias_node = std::dynamic_pointer_cast<AliasNode>(lqp);
      output_table_statistics = estimate_alias_node(*alias_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Join: {
      const auto join_node = std::dynamic_pointer_cast<JoinNode>(lqp);
      output_table_statistics =
          estimate_join_node(*join_node, left_input_table_statistics, right_input_table_statistics);
    } break;

    case LQPNodeType::Limit: {
      const auto limit_node = std::dynamic_pointer_cast<LimitNode>(lqp);
      output_table_statistics = estimate_limit_node(*limit_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Mock: {
      const auto mock_node = std::dynamic_pointer_cast<MockNode>(lqp);
      Assert(mock_node->table_statistics(), "Cannot return statistics of MockNode that was not assigned statistics");
      output_table_statistics = prune_column_statistics(mock_node->table_statistics(), mock_node->pruned_column_ids());
    } break;

    case LQPNodeType::Predicate: {
      const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(lqp);
      output_table_statistics = estimate_predicate_node(*predicate_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Projection: {
      const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(lqp);
      output_table_statistics = estimate_projection_node(*projection_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Sort: {
      output_table_statistics = left_input_table_statistics;
    } break;

    case LQPNodeType::StoredTable: {
      const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(lqp);

      const auto stored_table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      Assert(stored_table->table_statistics(), "Stored Table should have cardinality estimation statistics");

      if (stored_table_node->table_statistics) {
        // TableStatistics have changed from the original table's statistics
        Assert(stored_table_node->table_statistics->column_statistics.size() == stored_table->column_count(),
               "Statistics in StoredTableNode should have same number of columns as original table");
        output_table_statistics =
            prune_column_statistics(stored_table_node->table_statistics, stored_table_node->pruned_column_ids());
      } else {
        output_table_statistics =
            prune_column_statistics(stored_table->table_statistics(), stored_table_node->pruned_column_ids());
      }
    } break;

    case LQPNodeType::Validate: {
      const auto validate_node = std::dynamic_pointer_cast<ValidateNode>(lqp);
      output_table_statistics = estimate_validate_node(*validate_node, left_input_table_statistics);
    } break;

    case LQPNodeType::Union: {
      const auto union_node = std::dynamic_pointer_cast<UnionNode>(lqp);
      output_table_statistics =
          estimate_union_node(*union_node, left_input_table_statistics, right_input_table_statistics);
    } break;

    // These Node types should not be relevant during query optimization. Return an empty TableStatistics object for
    // them
    case LQPNodeType::CreateTable:
    case LQPNodeType::CreatePreparedPlan:
    case LQPNodeType::CreateView:
    case LQPNodeType::Update:
    case LQPNodeType::Insert:
    case LQPNodeType::Delete:
    case LQPNodeType::DropView:
    case LQPNodeType::DropTable:
    case LQPNodeType::StaticTable:
    case LQPNodeType::DummyTable: {
      auto empty_column_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>();
      output_table_statistics = std::make_shared<TableStatistics>(std::move(empty_column_statistics), Cardinality{0});
    } break;

    case LQPNodeType::Root:
      Fail("Cardinality of a node of this type should never be requested");
  }

  /**
   * 3. Store output_table_statistics in cache
   */
  if (join_graph_bitmask) {
    cardinality_estimation_cache.join_graph_statistics_cache->set(*join_graph_bitmask, lqp->column_expressions(),
                                                                  output_table_statistics);
  }

  if (cardinality_estimation_cache.statistics_by_lqp) {
    cardinality_estimation_cache.statistics_by_lqp->emplace(lqp, output_table_statistics);
  }

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_alias_node(
    const AliasNode& alias_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For AliasNodes, just reorder/remove AttributeStatistics from the input

  auto column_statistics =
      std::vector<std::shared_ptr<BaseAttributeStatistics>>{alias_node.column_expressions().size()};

  for (size_t expression_idx{0}; expression_idx < alias_node.column_expressions().size(); ++expression_idx) {
    const auto& expression = *alias_node.column_expressions()[expression_idx];
    const auto input_column_id = alias_node.left_input()->get_column_id(expression);
    column_statistics[expression_idx] = input_table_statistics->column_statistics[input_column_id];
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), input_table_statistics->row_count);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_projection_node(
    const ProjectionNode& projection_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For ProjectionNodes, reorder/remove AttributeStatistics from the input. They also perform calculations creating new
  // colums.
  // TODO(anybody) For columns newly created by a Projection no meaningful statistics can be generated yet, hence an
  //               empty AttributeStatistics object is created.

  auto column_statistics =
      std::vector<std::shared_ptr<BaseAttributeStatistics>>{projection_node.column_expressions().size()};

  for (size_t expression_idx{0}; expression_idx < projection_node.column_expressions().size(); ++expression_idx) {
    const auto& expression = *projection_node.column_expressions()[expression_idx];
    const auto input_column_id = projection_node.left_input()->find_column_id(expression);
    if (input_column_id) {
      column_statistics[expression_idx] = input_table_statistics->column_statistics[*input_column_id];
    } else {
      resolve_data_type(expression.data_type(), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        column_statistics[expression_idx] = std::make_shared<AttributeStatistics<ColumnDataType>>();
      });
    }
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), input_table_statistics->row_count);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_aggregate_node(
    const AggregateNode& aggregate_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For AggregateNodes, statistics from group-by columns are forwarded and for the aggregate columns
  // dummy statistics are created for now.

  auto column_statistics =
      std::vector<std::shared_ptr<BaseAttributeStatistics>>{aggregate_node.column_expressions().size()};

  for (size_t expression_idx{0}; expression_idx < aggregate_node.column_expressions().size(); ++expression_idx) {
    const auto& expression = *aggregate_node.column_expressions()[expression_idx];
    const auto input_column_id = aggregate_node.left_input()->find_column_id(expression);
    if (input_column_id) {
      column_statistics[expression_idx] = input_table_statistics->column_statistics[*input_column_id];
    } else {
      resolve_data_type(expression.data_type(), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        column_statistics[expression_idx] = std::make_shared<AttributeStatistics<ColumnDataType>>();
      });
    }
  }

  return std::make_shared<TableStatistics>(std::move(column_statistics), input_table_statistics->row_count);
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_validate_node(
    const ValidateNode& validate_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // Currently no statistics available to base ValidateNode on
  return input_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_predicate_node(
    const PredicateNode& predicate_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For PredicateNodes, the statistics of the columns scanned on are sliced and all other columns' statistics are
  // scaled with the estimated selectivity of the predicate.

  const auto predicate = predicate_node.predicate();

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
        std::vector<std::shared_ptr<BaseAttributeStatistics>>{input_table_statistics->column_statistics.size()};

    for (auto column_id = ColumnID{0}; column_id < output_column_statistics.size(); ++column_id) {
      output_column_statistics[column_id] =
          input_table_statistics->column_statistics[column_id]->scaled(PLACEHOLDER_SELECTIVITY_HIGH);
    }

    const auto row_count = Cardinality{input_table_statistics->row_count * PLACEHOLDER_SELECTIVITY_HIGH};
    return std::make_shared<TableStatistics>(std::move(output_column_statistics), row_count);
  }

  const auto operator_scan_predicates = OperatorScanPredicate::from_expression(*predicate, predicate_node);

  // TODO(anybody) Complex predicates are not processed right now and statistics objects are forwarded.
  //               That implies estimating a selectivity of 1 for such predicates
  if (!operator_scan_predicates) {
    return input_table_statistics;
  } else {
    auto output_table_statistics = input_table_statistics;

    for (const auto& operator_scan_predicate : *operator_scan_predicates) {
      output_table_statistics = estimate_operator_scan_predicate(output_table_statistics, operator_scan_predicate);
    }

    return output_table_statistics;
  }
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_join_node(
    const JoinNode& join_node, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics>& right_input_table_statistics) {
  // For inner-equi JoinNodes, a principle-of-inclusion algorithm is used.
  // The same algorithm is used for outer-equi JoinNodes, lacking a better alternative at the moment.
  // All other join modes and predicate conditions are treated as cross joins for now.

  if (join_node.join_mode == JoinMode::Cross) {
    return estimate_cross_join(*left_input_table_statistics, *right_input_table_statistics);
  } else {
    // TODO(anybody) Join cardinality estimation is consciously only performed for the primary join predicate. #1560
    const auto primary_operator_join_predicate = OperatorJoinPredicate::from_expression(
        *join_node.join_predicates()[0], *join_node.left_input(), *join_node.right_input());

    if (primary_operator_join_predicate) {
      switch (join_node.join_mode) {
        // For now, handle outer joins just as inner joins
        // TODO(anybody) Handle them more accurately, i.e., estimate how many tuples don't find matches. #1830
        case JoinMode::Left:
        case JoinMode::Right:
        case JoinMode::FullOuter:
        case JoinMode::Inner:
          switch (primary_operator_join_predicate->predicate_condition) {
            case PredicateCondition::Equals:
              return estimate_inner_equi_join(primary_operator_join_predicate->column_ids.first,
                                              primary_operator_join_predicate->column_ids.second,
                                              *left_input_table_statistics, *right_input_table_statistics);

            // TODO(anybody) Implement estimation for non-equi joins. #1830
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
          Fail("GCC thinks this is reachable");

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
    } else {
      // TODO(anybody) For now, estimate a selectivity of one. #1830
      return estimate_cross_join(*left_input_table_statistics, *right_input_table_statistics);
    }
  }

  Fail("GCC thinks this is reachable");
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_union_node(
    const UnionNode& union_node, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics>& right_input_table_statistics) {
  // Since UnionNodes are not really used right now, implementing an involved algorithm to union two TableStatistics
  // seems unjustified. For now, we just concatenate the two statistics objects

  DebugAssert(
      left_input_table_statistics->column_statistics.size() == right_input_table_statistics->column_statistics.size(),
      "Input TableStatistics need to have the same number of columns to perform a union");

  auto column_statistics = left_input_table_statistics->column_statistics;

  const auto row_count = Cardinality{left_input_table_statistics->row_count + right_input_table_statistics->row_count};

  const auto output_table_statistics = std::make_shared<TableStatistics>(std::move(column_statistics), row_count);

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_limit_node(
    const LimitNode& limit_node, const std::shared_ptr<TableStatistics>& input_table_statistics) {
  // For LimitNodes with a value as limit_expression, create a TableStatistics object with that value as row_count.
  // Otherwise, forward the input statistics for now.

  if (const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(limit_node.num_rows_expression())) {
    const auto row_count = lossy_variant_cast<float>(value_expression->value);
    if (!row_count) {
      // `value_expression->value` being NULL does not make much sense, but that is not the concern of the
      // CardinalityEstimator
      return input_table_statistics;
    }

    // Number of rows can never exceed number of input rows
    const auto clamped_row_count = std::min(*row_count, input_table_statistics->row_count);

    auto column_statistics =
        std::vector<std::shared_ptr<BaseAttributeStatistics>>{limit_node.column_expressions().size()};

    for (auto column_id = ColumnID{0}; column_id < input_table_statistics->column_statistics.size(); ++column_id) {
      resolve_data_type(input_table_statistics->column_data_type(column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;
        column_statistics[column_id] = std::make_shared<AttributeStatistics<ColumnDataType>>();
      });
    }

    return std::make_shared<TableStatistics>(std::move(column_statistics), clamped_row_count);
  } else {
    return input_table_statistics;
  }
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
      std::vector<std::shared_ptr<BaseAttributeStatistics>>{input_table_statistics->column_statistics.size()};

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto left_input_column_statistics =
        std::static_pointer_cast<AttributeStatistics<ColumnDataType>>(left_input_base_column_statistics);

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

        // All that remains of the column we scanned on are exclusively NULL values or exclusively non-NULL values
        const auto column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
        column_statistics->null_value_ratio = std::make_shared<NullValueRatioStatistics>(is_not_null ? 0.0f : 1.0f);
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

        if (left_data_type != right_data_type) {
          // TODO(anybody) Cannot estimate column-vs-column scan for differing data types, yet
          selectivity = PLACEHOLDER_SELECTIVITY_ALL;
          return;
        }

        if (predicate.predicate_condition != PredicateCondition::Equals) {
          // TODO(anyone) CardinalityEstimator cannot handle non-equi column-to-column scans right now
          selectivity = PLACEHOLDER_SELECTIVITY_ALL;
          return;
        }

        const auto right_input_column_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(
            input_table_statistics->column_statistics[*right_column_id]);

        const auto left_histogram = left_input_column_statistics->histogram;
        const auto right_histogram = right_input_column_statistics->histogram;
        if (!left_histogram || !right_histogram) {
          // Can only use histograms to estimate column-to-column scans right now
          // TODO(anyone) extend to other statistics objects
          selectivity = PLACEHOLDER_SELECTIVITY_ALL;
          return;
        }

        const auto bin_adjusted_left_histogram = left_histogram->split_at_bin_bounds(right_histogram->bin_bounds());
        const auto bin_adjusted_right_histogram = right_histogram->split_at_bin_bounds(left_histogram->bin_bounds());

        const auto column_vs_column_histogram = estimate_column_vs_column_equi_scan_with_histograms(
            *bin_adjusted_left_histogram, *bin_adjusted_right_histogram);
        if (!column_vs_column_histogram) {
          // No overlapping bins: No rows selected
          selectivity = 0.0f;
          return;
        }

        const auto cardinality = column_vs_column_histogram->total_count();
        selectivity = input_table_statistics->row_count == 0 ? 0.0f : cardinality / input_table_statistics->row_count;

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
                std::max(scan_statistics_object->total_distinct_count(), HistogramCountType{1.0f});
            selectivity = total_distinct_count > 0 ? 1.0f / total_distinct_count : 0.0f;
          } break;

          case PredicateCondition::NotEquals: {
            const auto total_distinct_count =
                std::max(scan_statistics_object->total_distinct_count(), HistogramCountType{1.0f});
            selectivity = total_distinct_count > 0 ? (total_distinct_count - 1.0f) / total_distinct_count : 0.0f;
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
         * Estimate ColumnVsValue
         */
        Assert(predicate.value.type() == typeid(AllTypeVariant), "Expected AllTypeVariant");

        const auto value_variant = boost::get<AllTypeVariant>(predicate.value);
        if (variant_is_null(value_variant)) {
          // A predicate `<column> <condition> NULL` always has a selectivity of 0
          selectivity = 0.0f;
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
          selectivity = 0.0f;
          return;
        }

        // TODO(anybody) Simplify this block if AbstractStatisticsObject ever supports total_count()
        const auto sliced_histogram =
            std::dynamic_pointer_cast<AbstractHistogram<ColumnDataType>>(sliced_statistics_object);
        DebugAssert(sliced_histogram, "Expected slicing of a Histogram to return either nullptr or a Histogram");
        if (input_table_statistics->row_count == 0 || sliced_histogram->total_count() == 0.0f) {
          selectivity = 0.0f;
        } else {
          selectivity = sliced_histogram->total_count() / scan_statistics_object->total_count();
        }

        const auto column_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
        column_statistics->set_statistics_object(sliced_statistics_object);

        output_column_statistics[left_column_id] = column_statistics;
      }
    }
  });

  // Entire chunk matches; simply return the input
  if (selectivity == 1) {
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
   * Column-to-column scan estimation is notoriously hard, selectivities from 0 to 1 are possible for the same histogram
   * pairs.
   * Thus, we do the most conservative estimation and compute the upper bound of value- and distinct counts for each
   * bin pair.
   */

  auto left_idx = BinID{0};
  auto right_idx = BinID{0};
  auto left_bin_count = left_histogram.bin_count();
  auto right_bin_count = right_histogram.bin_count();

  GenericHistogramBuilder<T> builder;

  for (; left_idx < left_bin_count && right_idx < right_bin_count;) {
    const auto left_min = left_histogram.bin_minimum(left_idx);
    const auto right_min = right_histogram.bin_minimum(right_idx);

    if (left_min < right_min) {
      ++left_idx;
      continue;
    }

    if (right_min < left_min) {
      ++right_idx;
      continue;
    }

    DebugAssert(left_histogram.bin_maximum(left_idx) == right_histogram.bin_maximum(right_idx),
                "Histogram bin boundaries do not match");

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

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_inner_equi_join(
    const ColumnID left_column_id, const ColumnID right_column_id, const TableStatistics& left_input_table_statistics,
    const TableStatistics& right_input_table_statistics) {
  const auto left_data_type = left_input_table_statistics.column_data_type(left_column_id);
  const auto right_data_type = right_input_table_statistics.column_data_type(right_column_id);

  // We expect both columns to be of the same type. This allows us to resolve the type only once, reducing the
  // compile time. For differing column types and/or string columns (which we cannot handle right now), we assume that
  // all tuples qualify. This is probably a gross overestimation, but we need to return something...
  // TODO(anybody) - Implement join estimation for differing column data types
  //               - Implement join estimation for String columns
  if (left_data_type != right_data_type || left_data_type == DataType::String) {
    return estimate_cross_join(left_input_table_statistics, right_input_table_statistics);
  }

  std::shared_ptr<TableStatistics> output_table_statistics;

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto left_input_column_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(
        left_input_table_statistics.column_statistics[left_column_id]);
    const auto right_input_column_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(
        right_input_table_statistics.column_statistics[right_column_id]);

    auto cardinality = Cardinality{0};
    auto join_column_histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

    auto left_histogram = left_input_column_statistics->histogram;
    auto right_histogram = right_input_column_statistics->histogram;

    if (left_histogram && right_histogram) {
      // If we have histograms, we use the principle of inclusion to determine the number of matches between two bins.
      join_column_histogram = estimate_inner_equi_join_with_histograms(*left_histogram, *right_histogram);
      cardinality = join_column_histogram->total_count();
    } else {
      // TODO(anybody) If there aren't histograms on both sides, use some other algorithm/statistics to estimate the
      //               Join
      cardinality = left_input_table_statistics.row_count * right_input_table_statistics.row_count;
    }

    const auto left_selectivity = Selectivity{
        left_input_table_statistics.row_count > 0 ? cardinality / left_input_table_statistics.row_count : 0.0f};
    const auto right_selectivity = Selectivity{
        right_input_table_statistics.row_count > 0 ? cardinality / right_input_table_statistics.row_count : 0.0f};

    /**
     * Write out the AttributeStatistics of all output columns. With no correlation info available, simply scale all
     * those that didn't participate in the join predicate
     */
    std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics{
        left_input_table_statistics.column_statistics.size() + right_input_table_statistics.column_statistics.size()};

    const auto left_column_count = left_input_table_statistics.column_statistics.size();

    const auto join_columns_output_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
    join_columns_output_statistics->histogram = join_column_histogram;
    column_statistics[left_column_id] = join_columns_output_statistics;
    column_statistics[left_column_count + right_column_id] = join_columns_output_statistics;

    for (auto column_id = ColumnID{0}; column_id < left_column_count; ++column_id) {
      if (column_statistics[column_id]) continue;

      column_statistics[column_id] = left_input_table_statistics.column_statistics[column_id]->scaled(left_selectivity);
    }
    for (auto column_id = ColumnID{0}; column_id < right_input_table_statistics.column_statistics.size(); ++column_id) {
      if (column_statistics[left_column_count + column_id]) continue;

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
  // TODO(anybody) - Implement join estimation for differing column data types
  //               - Implement join estimation for String columns
  if (left_data_type != right_data_type || left_data_type == DataType::String) {
    return std::make_shared<TableStatistics>(left_input_table_statistics);
  }

  std::shared_ptr<TableStatistics> output_table_statistics;

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto left_input_column_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(
        left_input_table_statistics.column_statistics[left_column_id]);
    const auto right_input_column_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(
        right_input_table_statistics.column_statistics[right_column_id]);

    auto cardinality = Cardinality{0};
    auto join_column_histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

    auto left_histogram = left_input_column_statistics->histogram;
    auto right_histogram = right_input_column_statistics->histogram;

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
      // TODO(anybody) If there aren't histograms on both sides, use some other algorithm/statistics to estimate the
      //               Join
      cardinality = left_input_table_statistics.row_count;
    }

    const auto left_selectivity = Selectivity{
        left_input_table_statistics.row_count > 0 ? cardinality / left_input_table_statistics.row_count : 0.0f};

    /**
     * Write out the AttributeStatistics of all output columns. With no correlation info available, simply scale all
     * those that didn't participate in the join predicate
     */
    std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics{
        left_input_table_statistics.column_statistics.size()};

    const auto left_column_count = left_input_table_statistics.column_statistics.size();

    const auto join_columns_output_statistics = std::make_shared<AttributeStatistics<ColumnDataType>>();
    join_columns_output_statistics->histogram = join_column_histogram;
    column_statistics[left_column_id] = join_columns_output_statistics;

    for (auto column_id = ColumnID{0}; column_id < left_column_count; ++column_id) {
      if (column_statistics[column_id]) continue;

      column_statistics[column_id] = left_input_table_statistics.column_statistics[column_id]->scaled(left_selectivity);
    }

    output_table_statistics = std::make_shared<TableStatistics>(std::move(column_statistics), cardinality);
  });

  Assert(output_table_statistics->row_count <= left_input_table_statistics.row_count * 1.01f,
         "Semi join should not increase cardinality");

  return output_table_statistics;
}

std::shared_ptr<TableStatistics> CardinalityEstimator::estimate_cross_join(
    const TableStatistics& left_input_table_statistics, const TableStatistics& right_input_table_statistics) {
  // Every tuple from the left side gets emitted once for each tuple on the right side - and vice versa
  const auto left_selectivity = Selectivity{right_input_table_statistics.row_count};
  const auto right_selectivity = Selectivity{left_input_table_statistics.row_count};

  /**
   * Scale up the input AttributeStatistics with the selectivities specified above and write them to the output
   * TableStatistics
   */
  std::vector<std::shared_ptr<BaseAttributeStatistics>> column_statistics{
      left_input_table_statistics.column_statistics.size() + right_input_table_statistics.column_statistics.size()};

  const auto left_column_count = left_input_table_statistics.column_statistics.size();
  for (auto column_id = ColumnID{0}; column_id < left_column_count; ++column_id) {
    column_statistics[column_id] = left_input_table_statistics.column_statistics[column_id]->scaled(left_selectivity);
  }

  for (auto column_id = ColumnID{0}; column_id < right_input_table_statistics.column_statistics.size(); ++column_id) {
    column_statistics[left_column_count + column_id] =
        right_input_table_statistics.column_statistics[column_id]->scaled(right_selectivity);
  }

  const auto row_count = Cardinality{left_selectivity * right_selectivity};

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

  GenericHistogramBuilder<T> builder;

  // Iterate over both unified histograms and find overlapping bins
  for (; left_idx < left_bin_count && right_idx < right_bin_count;) {
    const auto left_min = unified_left_histogram->bin_minimum(left_idx);
    const auto right_min = unified_right_histogram->bin_minimum(right_idx);

    if (left_min < right_min) {
      ++left_idx;
      continue;
    }

    if (right_min < left_min) {
      ++right_idx;
      continue;
    }

    DebugAssert(unified_left_histogram->bin_maximum(left_idx) == unified_right_histogram->bin_maximum(right_idx),
                "Histogram bin boundaries do not match");

    // Overlapping bins found, estimate the join for these bins' range
    const auto [height, distinct_count] = estimate_inner_equi_join_of_bins(  // NOLINT
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

std::pair<HistogramCountType, HistogramCountType> CardinalityEstimator::estimate_inner_equi_join_of_bins(
    const float left_height, const float left_distinct_count, const float right_height,
    const float right_distinct_count) {
  // Range with more distinct values should be on the left side to keep the algorithm below simple
  if (left_distinct_count < right_distinct_count) {
    return estimate_inner_equi_join_of_bins(right_height, right_distinct_count, left_height, left_distinct_count);
  }

  // Early out to avoid division by zero below
  if (left_distinct_count == 0 || right_distinct_count == 0) {
    return {HistogramCountType{0.0f}, HistogramCountType{0.0f}};
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

std::shared_ptr<TableStatistics> CardinalityEstimator::prune_column_statistics(
    const std::shared_ptr<TableStatistics>& table_statistics, const std::vector<ColumnID>& pruned_column_ids) {
  if (pruned_column_ids.empty()) {
    return table_statistics;
  }

  /**
   * Prune `pruned_column_ids` from the statistics
   */

  auto output_column_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>(
      table_statistics->column_statistics.size() - pruned_column_ids.size());

  auto pruned_column_ids_iter = pruned_column_ids.begin();

  for (auto input_column_id = ColumnID{0}, output_column_id = ColumnID{0};
       input_column_id < table_statistics->column_statistics.size(); ++input_column_id) {
    // Skip `stored_column_id` if it is in the sorted vector `_pruned_column_ids`
    if (pruned_column_ids_iter != pruned_column_ids.end() && input_column_id == *pruned_column_ids_iter) {
      ++pruned_column_ids_iter;
      continue;
    }

    output_column_statistics[output_column_id] = table_statistics->column_statistics[input_column_id];
    ++output_column_id;
  }

  return std::make_shared<TableStatistics>(std::move(output_column_statistics), table_statistics->row_count);
}

}  // namespace opossum
