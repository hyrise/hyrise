#include "cardinality_estimator.hpp"

#include <iostream>
#include <memory>

#include "chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "chunk_statistics/histograms/equal_width_histogram.hpp"
#include "chunk_statistics/histograms/generic_histogram.hpp"
#include "chunk_statistics/histograms/single_bin_histogram.hpp"
#include "chunk_statistics2.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_select_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "optimizer/optimization_context.hpp"
#include "resolve_type.hpp"
#include "segment_statistics2.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_statistics2.hpp"
#include "utils/assert.hpp"

namespace {

using namespace opossum;  // NOLINT

void populate_cache_bitmask(const std::shared_ptr<AbstractLQPNode>& plan,
                            const std::shared_ptr<OptimizationContext>& context,
                            std::optional<boost::dynamic_bitset<>>& bitmask) {
  if (!bitmask) return;

  visit_lqp(plan, [&](const auto& node) {
    if (!bitmask) return LQPVisitation::DoNotVisitInputs;

    if (node->input_count() == 0) {
      const auto leaf_iter = context->plan_leaf_indices.find(node);
      Assert(leaf_iter != context->plan_leaf_indices.end(), "Didn't expect new leaf");
      Assert(leaf_iter->second < bitmask->size(), "");
      bitmask->set(leaf_iter->second);
    } else if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
      const auto predicate_index_iter = context->predicate_indices.find(join_node->join_predicate());
      if (predicate_index_iter == context->predicate_indices.end()) {
        bitmask.reset();
        return LQPVisitation::DoNotVisitInputs;
      }

      Assert(predicate_index_iter->second + context->plan_leaf_indices.size() < bitmask->size(), "");
      bitmask->set(predicate_index_iter->second + context->plan_leaf_indices.size());
    } else if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
      const auto predicate_index_iter = context->predicate_indices.find(predicate_node->predicate());
      if (predicate_index_iter == context->predicate_indices.end()) {
        bitmask.reset();
        return LQPVisitation::DoNotVisitInputs;
      }

      Assert(predicate_index_iter->second + context->plan_leaf_indices.size() < bitmask->size(), "");
      bitmask->set(predicate_index_iter->second + context->plan_leaf_indices.size());
    } else if (node->type == LQPNodeType::Validate || node->type == LQPNodeType::Sort) {
      // ignore node type as it doesn't change the cardinality
    } else {
      bitmask.reset();
      return LQPVisitation::DoNotVisitInputs;
    }

    for (const auto& node_expression : node->node_expressions) {
      visit_expression(node_expression, [&](const auto& sub_expression) {
        if (const auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression)) {
          populate_cache_bitmask(select_expression->lqp, context, bitmask);
        }
        return ExpressionVisitation::VisitArguments;
      });
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace

namespace opossum {

std::optional<boost::dynamic_bitset<>> CardinalityEstimator::build_plan_bitmask(
    const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<OptimizationContext>& context) {
  auto bitmask =
      std::optional<boost::dynamic_bitset<>>{context->plan_leaf_indices.size() + context->predicate_indices.size()};
  populate_cache_bitmask(lqp, context, bitmask);
  return bitmask;
}

Cardinality CardinalityEstimator::estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                       const std::shared_ptr<OptimizationContext>& context) const {
  std::cout << "CardinalityEstimator::estimate_cardinality() {" << std::endl;
  lqp->print();
  std::cout << "}" << std::endl;
  return estimate_statistics(lqp, context)->row_count();
}

std::shared_ptr<TableStatistics2> CardinalityEstimator::estimate_statistics(
    const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<OptimizationContext>& context) const {
  auto bitmask = std::optional<boost::dynamic_bitset<>>{};

  if (context) {
    bitmask = build_plan_bitmask(lqp, context);
    if (bitmask) {
      const auto cache_iter = context->cache.find(*bitmask);
      if (cache_iter != context->cache.end()) {
        std::cout << "Found statistics for " << *bitmask << " in cache" << std::endl;
        const auto& cache_entry = cache_iter->second;

        const auto cached_table_statistics = cache_entry.table_statistics;
        const auto table_statistics = std::make_shared<TableStatistics2>();

        table_statistics->chunk_statistics.resize(cache_entry.table_statistics->chunk_statistics.size());
        for (auto chunk_id = ChunkID{0}; chunk_id < table_statistics->chunk_statistics.size(); ++chunk_id) {
          const auto& cached_chunk_statistics = cached_table_statistics->chunk_statistics[chunk_id];
          table_statistics->chunk_statistics[chunk_id] =
              std::make_shared<ChunkStatistics2>(cached_chunk_statistics->row_count);
          table_statistics->chunk_statistics[chunk_id]->segment_statistics.resize(cached_chunk_statistics->segment_statistics.size());
          table_statistics->chunk_statistics[chunk_id]->approx_invalid_row_count =
              cached_chunk_statistics->approx_invalid_row_count;
        }

        const auto& column_expressions = lqp->column_expressions();

        for (auto column_id = ColumnID{0}; column_id < column_expressions.size(); ++column_id) {
          const auto column_id_iter = cache_entry.column_expressions.find(column_expressions[column_id]);
          Assert(column_id_iter != cache_entry.column_expressions.end(), "Column not found in cached statistics");
          const auto cached_column_id = column_id_iter->second;

          for (auto chunk_id = ChunkID{0};chunk_id < table_statistics->chunk_statistics.size(); ++chunk_id ) {
            table_statistics->chunk_statistics[chunk_id]->segment_statistics[column_id] =
            cached_table_statistics->chunk_statistics[chunk_id]->segment_statistics[cached_column_id];
          }
        }

        return table_statistics;
      } else {
        std::cout << "Found no statistics for " << *bitmask << " in cache" << std::endl;
      }
    } else {
      std::cout << "No bitmask" << std::endl;
    }
  } else {
    std::cout << "No context" << std::endl;
  }

  auto output_table_statistics = std::shared_ptr<TableStatistics2>{};

  if (const auto alias_node = std::dynamic_pointer_cast<AliasNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input(), context);
    output_table_statistics = std::make_shared<TableStatistics2>();
    output_table_statistics->chunk_statistics.reserve(input_table_statistics->chunk_statistics.size());

    for (const auto& input_chunk_statistics : input_table_statistics->chunk_statistics) {
      const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(input_chunk_statistics->row_count);
      output_chunk_statistics->segment_statistics.reserve(alias_node->column_expressions().size());

      for (const auto& expression : alias_node->column_expressions()) {
        const auto input_column_id = alias_node->left_input()->get_column_id(*expression);
        output_chunk_statistics->segment_statistics.emplace_back(
            input_chunk_statistics->segment_statistics[input_column_id]);
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }
  }

  if (const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input(), context);
    output_table_statistics = std::make_shared<TableStatistics2>();
    output_table_statistics->chunk_statistics.reserve(input_table_statistics->chunk_statistics.size());

    for (const auto& input_chunk_statistics : input_table_statistics->chunk_statistics) {
      const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(input_chunk_statistics->row_count);
      output_chunk_statistics->segment_statistics.reserve(projection_node->column_expressions().size());

      for (const auto& expression : projection_node->column_expressions()) {
        const auto input_column_id = projection_node->left_input()->find_column_id(*expression);
        if (input_column_id) {
          output_chunk_statistics->segment_statistics.emplace_back(
              input_chunk_statistics->segment_statistics[*input_column_id]);
        } else {
          resolve_data_type(expression->data_type(), [&](const auto data_type_t) {
            using ColumnDataType = typename decltype(data_type_t)::type;
            output_chunk_statistics->segment_statistics.emplace_back(
                std::make_shared<SegmentStatistics2<ColumnDataType>>());
          });
        }
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }
  }

  if (const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input(), context);
    output_table_statistics = std::make_shared<TableStatistics2>();
    output_table_statistics->chunk_statistics.reserve(input_table_statistics->chunk_statistics.size());

    for (const auto& input_chunk_statistics : input_table_statistics->chunk_statistics) {
      const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(input_chunk_statistics->row_count);
      output_chunk_statistics->segment_statistics.reserve(aggregate_node->column_expressions().size());

      for (const auto& expression : aggregate_node->column_expressions()) {
        const auto input_column_id = aggregate_node->left_input()->find_column_id(*expression);
        if (input_column_id) {
          output_chunk_statistics->segment_statistics.emplace_back(
              input_chunk_statistics->segment_statistics[*input_column_id]);
        } else {
          resolve_data_type(expression->data_type(), [&](const auto data_type_t) {
            using ColumnDataType = typename decltype(data_type_t)::type;
            output_chunk_statistics->segment_statistics.emplace_back(
                std::make_shared<SegmentStatistics2<ColumnDataType>>());
          });
        }
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }
  }

  if (const auto validate_node = std::dynamic_pointer_cast<ValidateNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input(), context);
    output_table_statistics = std::make_shared<TableStatistics2>();
    output_table_statistics->chunk_statistics.reserve(input_table_statistics->chunk_statistics.size());

    for (const auto& input_chunk_statistics : input_table_statistics->chunk_statistics) {
      if (input_chunk_statistics->row_count == 0) {
        // No need to write out statistics for Chunks without rows
        continue;
      }

      const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(
          input_chunk_statistics->row_count - input_chunk_statistics->approx_invalid_row_count);
      output_chunk_statistics->segment_statistics.reserve(input_chunk_statistics->segment_statistics.size());

      const auto selectivity = (input_chunk_statistics->row_count - input_chunk_statistics->approx_invalid_row_count) /
                               input_chunk_statistics->row_count;

      for (const auto& segment_statistics : input_chunk_statistics->segment_statistics) {
        output_chunk_statistics->segment_statistics.emplace_back(
            segment_statistics->scale_with_selectivity(selectivity));
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }
  }

  if (std::dynamic_pointer_cast<SortNode>(lqp)) {
    output_table_statistics = estimate_statistics(lqp->left_input(), context);
  }

  if (const auto mock_node = std::dynamic_pointer_cast<MockNode>(lqp)) {
    Assert(mock_node->table_statistics2(), "");
    output_table_statistics = mock_node->table_statistics2();
  }

  if (const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(lqp)) {
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    Assert(table->table_statistics2(), "");
    output_table_statistics = table->table_statistics2();
  }

  if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input(), context);
    output_table_statistics = std::make_shared<TableStatistics2>();

    const auto operator_scan_predicates =
        OperatorScanPredicate::from_expression(*predicate_node->predicate(), *predicate_node);

    // TODO(anybody) Complex predicates are not processed right now and statistics objects are forwarded
    //               That implies estimating a selectivity of 1 for such predicates
    if (!operator_scan_predicates) {
      output_table_statistics = input_table_statistics;
    } else {
      for (const auto& input_chunk_statistics : input_table_statistics->chunk_statistics) {
        const auto output_chunk_statistics =
            estimate_scan_predicates_on_chunk(predicate_node, input_chunk_statistics, *operator_scan_predicates);
        // If there are no matches expected in this Chunk, don't return a ChunkStatistics object for this
        // Chunk
        if (output_chunk_statistics) {
          output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
        }
      }
    }
  }

  if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(lqp)) {
    std::cout << "EstimateJoinNode " << join_node->description() << "{" << std::endl;

    const auto left_input_table_statistics = estimate_statistics(lqp->left_input(), context);
    const auto right_input_table_statistics = estimate_statistics(lqp->right_input(), context);
    output_table_statistics = std::make_shared<TableStatistics2>();
    output_table_statistics->chunk_statistics.reserve(left_input_table_statistics->chunk_statistics.size() *
                                                      right_input_table_statistics->chunk_statistics.size());

    switch (join_node->join_mode) {
      // TODO(anybody) Handle properly
      case JoinMode::Semi:
      case JoinMode::Anti: {
        output_table_statistics = left_input_table_statistics;
      } break;
      
      // TODO(anybody) For now, handle outer joins just as inner joins
      case JoinMode::Left:
      case JoinMode::Right:
      case JoinMode::Outer:
      case JoinMode::Inner: {
        const auto operator_join_predicate = OperatorJoinPredicate::from_join_node(*join_node);
        if (!operator_join_predicate) {
          output_table_statistics = estimate_cross_join(left_input_table_statistics, right_input_table_statistics);
        } else {
          Assert(operator_join_predicate->predicate_condition == PredicateCondition::Equals, "NYI");

          const auto left_column_id = operator_join_predicate->column_ids.first;
          const auto right_column_id = operator_join_predicate->column_ids.second;
          const auto left_data_type = join_node->left_input()->column_expressions().at(left_column_id)->data_type();
          const auto right_data_type = join_node->right_input()->column_expressions().at(right_column_id)->data_type();
          Assert(left_data_type == right_data_type, "NYI");

          resolve_data_type(left_data_type, [&](const auto data_type_t) {
            using ColumnDataType = typename decltype(data_type_t)::type;

            // TODO(anybody) For many Chunks on both sides this nested loop will be inefficient.
            //               Consider approaches to merge statistic objects on each side.

            std::cout << "left_input_table_statistics: " << left_input_table_statistics->chunk_statistics.size()
                      << " chunks" << std::endl;
            std::cout << "right_input_table_statistics: " << right_input_table_statistics->chunk_statistics.size()
                      << " chunks" << std::endl;

            for (const auto& left_input_chunk_statistics : left_input_table_statistics->chunk_statistics) {
              for (const auto& right_input_chunk_statistics : right_input_table_statistics->chunk_statistics) {
                const auto left_input_segment_statistics =
                    std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
                        left_input_chunk_statistics->segment_statistics[left_column_id]);
                const auto right_input_segment_statistics =
                    std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
                        right_input_chunk_statistics->segment_statistics[right_column_id]);

                auto cardinality = Cardinality{0};
                auto join_column_histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

                if (left_data_type == DataType::String) {
                  cardinality = left_input_chunk_statistics->row_count * right_input_chunk_statistics->row_count;
                } else {
                  const auto left_histogram =
                      CardinalityEstimator::get_best_available_histogram(*left_input_segment_statistics);
                  const auto right_histogram =
                      CardinalityEstimator::get_best_available_histogram(*right_input_segment_statistics);
                  Assert(left_histogram && right_histogram, "NYI");

                  const auto unified_left_histogram = left_histogram->split_at_bin_edges(right_histogram->bin_edges());
                  const auto unified_right_histogram = right_histogram->split_at_bin_edges(left_histogram->bin_edges());

                  join_column_histogram = estimate_histogram_of_inner_equi_join_with_bin_adjusted_histograms(
                      unified_left_histogram, unified_right_histogram);

                  std::cout << "left_histogram: " << left_histogram->description() << std::endl;
                  std::cout << "right_histogram: " << right_histogram->description() << std::endl;
                  std::cout << "unified_left_histogram: " << unified_left_histogram->description() << std::endl;
                  std::cout << "unified_right_histogram: " << unified_right_histogram->description() << std::endl;
                  if (join_column_histogram)
                    std::cout << "join_column_histogram: " << join_column_histogram->description() << std::endl;
                  std::cout << std::endl;

                  if (!join_column_histogram) continue;

                  cardinality = join_column_histogram->total_count();
                }

                const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(cardinality);
                output_chunk_statistics->segment_statistics.reserve(
                    left_input_chunk_statistics->segment_statistics.size() +
                    right_input_chunk_statistics->segment_statistics.size());

                const auto left_selectivity = cardinality / left_input_chunk_statistics->row_count;
                const auto right_selectivity = cardinality / right_input_chunk_statistics->row_count;

                /**
                 * Write out the SegmentStatistics
                 */
                for (auto column_id = ColumnID{0}; column_id < left_input_chunk_statistics->segment_statistics.size();
                     ++column_id) {
                  if (join_column_histogram && column_id == left_column_id) {
                    const auto segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
                    segment_statistics->set_statistics_object(join_column_histogram);
                    output_chunk_statistics->segment_statistics.emplace_back(segment_statistics);
                  } else {
                    const auto& left_segment_statistics = left_input_chunk_statistics->segment_statistics[column_id];
                    output_chunk_statistics->segment_statistics.emplace_back(
                        left_segment_statistics->scale_with_selectivity(left_selectivity));
                  }
                }

                for (auto column_id = ColumnID{0}; column_id < right_input_chunk_statistics->segment_statistics.size();
                     ++column_id) {
                  if (join_column_histogram && column_id == right_column_id) {
                    const auto segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
                    segment_statistics->set_statistics_object(join_column_histogram);
                    output_chunk_statistics->segment_statistics.emplace_back(segment_statistics);
                  } else {
                    const auto& right_segment_statistics = right_input_chunk_statistics->segment_statistics[column_id];
                    output_chunk_statistics->segment_statistics.emplace_back(
                        right_segment_statistics->scale_with_selectivity(right_selectivity));
                  }
                }

                output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
              }
            }
          });
        }
      } break;

      case JoinMode::Cross:
        output_table_statistics = estimate_cross_join(left_input_table_statistics, right_input_table_statistics);
        break;

      default:
        Fail("JoinMode not implemented");
    }

    std::cout << "}" << std::endl;
  }

  Assert(output_table_statistics, "NYI");

  if (bitmask) {
    std::cout << "Storing statistics for " << *bitmask << " in cache" << std::endl;

    auto cache_entry = OptimizationContext::TableStatisticsCacheEntry{};
    cache_entry.table_statistics = output_table_statistics;

    const auto& column_expressions = lqp->column_expressions();
    for (auto column_id = ColumnID{0}; column_id < column_expressions.size(); ++column_id) {
      cache_entry.column_expressions.emplace(column_expressions[column_id], column_id);
    }

    context->cache.emplace(*bitmask, std::move(cache_entry));
  }

  return output_table_statistics;
}

std::shared_ptr<ChunkStatistics2> CardinalityEstimator::estimate_scan_predicates_on_chunk(
    const std::shared_ptr<PredicateNode>& predicate_node,
    const std::shared_ptr<ChunkStatistics2>& input_chunk_statistics,
    const std::vector<OperatorScanPredicate>& operator_scan_predicates) {
  auto output_chunk_statistics = input_chunk_statistics;

  for (const auto& operator_scan_predicate : operator_scan_predicates) {
    // TODO(anybody) use IsNullStatisticsObject
    if (operator_scan_predicate.predicate_condition == PredicateCondition::IsNull) {
      return nullptr;
    } else if (operator_scan_predicate.predicate_condition == PredicateCondition::IsNotNull) {
      continue;
    }

    const auto predicate_input_chunk_statistics = output_chunk_statistics;
    auto predicate_output_chunk_statistics = std::make_shared<ChunkStatistics2>();
    predicate_output_chunk_statistics->segment_statistics.resize(
        predicate_input_chunk_statistics->segment_statistics.size());

    auto predicate_chunk_selectivity = Selectivity{1};

    const auto left_column_id = operator_scan_predicate.column_id;
    auto right_column_id = std::optional<ColumnID>{};

    /**
     * Manipulate statistics of column that we scan on
     */
    const auto base_segment_statistics = output_chunk_statistics->segment_statistics.at(left_column_id);

    const auto data_type = predicate_node->column_expressions().at(left_column_id)->data_type();

    resolve_data_type(data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      const auto segment_statistics =
          std::static_pointer_cast<SegmentStatistics2<ColumnDataType>>(base_segment_statistics);

      auto primary_scan_statistics_object = CardinalityEstimator::get_best_available_histogram(*segment_statistics);
      // If there are no statistics available for this segment, assume a selectivity of 1
      if (!primary_scan_statistics_object) {
        predicate_output_chunk_statistics = predicate_input_chunk_statistics;
        return;
      }

      if (operator_scan_predicate.value.type() == typeid(ColumnID)) {
        right_column_id = boost::get<ColumnID>(operator_scan_predicate.value);

        if (predicate_node->column_expressions().at(left_column_id)->data_type() !=
            predicate_node->column_expressions().at(*right_column_id)->data_type()) {
          predicate_output_chunk_statistics = predicate_input_chunk_statistics;
          return;
        }

        if (operator_scan_predicate.predicate_condition != PredicateCondition::Equals) {
          // TODO(anyone) CardinalityEstimator cannot handle non-equi column-to-column scans right now
          predicate_output_chunk_statistics = predicate_input_chunk_statistics;
          return;
        }

        const auto left_input_segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
            input_chunk_statistics->segment_statistics[left_column_id]);
        const auto right_input_segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
            input_chunk_statistics->segment_statistics[*right_column_id]);

        const auto left_histogram = CardinalityEstimator::get_best_available_histogram(*left_input_segment_statistics);
        const auto right_histogram =
            CardinalityEstimator::get_best_available_histogram(*right_input_segment_statistics);
        Assert(left_histogram && right_histogram, "NYI");

        const auto bin_adjusted_left_histogram = left_histogram->split_at_bin_edges(right_histogram->bin_edges());
        const auto bin_adjusted_right_histogram = right_histogram->split_at_bin_edges(left_histogram->bin_edges());

        const auto column_to_column_histogram =
            estimate_histogram_of_column_to_column_equi_scan_with_bin_adjusted_histograms(bin_adjusted_left_histogram,
                                                                                          bin_adjusted_right_histogram);
        if (!column_to_column_histogram) {
          // No matches in this Chunk estimated; prune the ChunkStatistics
          predicate_output_chunk_statistics = nullptr;
          return;
        }

        const auto cardinality = column_to_column_histogram->total_count();
        predicate_chunk_selectivity = cardinality / input_chunk_statistics->row_count;

        /**
         * Write out the SegmentStatistics
         */
        const auto output_segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
        output_segment_statistics->set_statistics_object(column_to_column_histogram);
        predicate_output_chunk_statistics->segment_statistics[left_column_id] = output_segment_statistics;
        predicate_output_chunk_statistics->segment_statistics[*right_column_id] = output_segment_statistics;

      } else if (operator_scan_predicate.value.type() == typeid(ParameterID)) {
        predicate_output_chunk_statistics = predicate_input_chunk_statistics;
        return;

      } else {
        Assert(operator_scan_predicate.value.type() == typeid(AllTypeVariant), "Expected AllTypeVariant");

        auto value2_all_type_variant = std::optional<AllTypeVariant>{};
        if (operator_scan_predicate.value2) {
          Assert(operator_scan_predicate.value2->type() == typeid(AllTypeVariant),
                 "Histogram can't handle column-to-column scans right now");
          value2_all_type_variant = boost::get<AllTypeVariant>(*operator_scan_predicate.value2);
        }

        const auto sliced_statistics_object = primary_scan_statistics_object->slice_with_predicate(
            operator_scan_predicate.predicate_condition, boost::get<AllTypeVariant>(operator_scan_predicate.value),
            value2_all_type_variant);

        const auto cardinality_estimate = primary_scan_statistics_object->estimate_cardinality(
            operator_scan_predicate.predicate_condition, boost::get<AllTypeVariant>(operator_scan_predicate.value),
            value2_all_type_variant);

        if (predicate_input_chunk_statistics->row_count == 0 ||
            cardinality_estimate.type == EstimateType::MatchesNone) {
          // No matches in this Chunk estimated; prune the ChunkStatistics
          predicate_output_chunk_statistics = nullptr;
          return;
        } else {
          predicate_chunk_selectivity =
              cardinality_estimate.cardinality / primary_scan_statistics_object->total_count();
        }

        const auto output_segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
        output_segment_statistics->set_statistics_object(sliced_statistics_object);

        predicate_output_chunk_statistics->segment_statistics[left_column_id] = output_segment_statistics;
      }
    });

    // No matches in this Chunk estimated; prune the ChunkStatistics
    if (!predicate_output_chunk_statistics) return nullptr;

    // If predicate has a selectivity of != 1, scale the other columns' SegmentStatistics
    if (predicate_output_chunk_statistics != predicate_input_chunk_statistics) {
      /**
       * Manipulate statistics of all columns that we DIDN'T scan on with this predicate
       */
      for (auto column_id = ColumnID{0}; column_id < predicate_input_chunk_statistics->segment_statistics.size();
           ++column_id) {
        if (column_id == left_column_id || (right_column_id && column_id == *right_column_id)) continue;

        predicate_output_chunk_statistics->segment_statistics[column_id] =
            predicate_input_chunk_statistics->segment_statistics[column_id]->scale_with_selectivity(
                predicate_chunk_selectivity);
      }

      /**
       * Adjust ChunkStatistics row_count
       */
      predicate_output_chunk_statistics->row_count =
          predicate_input_chunk_statistics->row_count * predicate_chunk_selectivity;
    }

    output_chunk_statistics = predicate_output_chunk_statistics;
  }

  return output_chunk_statistics;
}

std::shared_ptr<TableStatistics2> CardinalityEstimator::estimate_cross_join(
    const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics2>& right_input_table_statistics) {
  const auto output_table_statistics = std::make_shared<TableStatistics2>();

  for (const auto& left_input_chunk_statistics : left_input_table_statistics->chunk_statistics) {
    for (const auto& right_input_chunk_statistics : right_input_table_statistics->chunk_statistics) {
      const auto left_selectivity = right_input_chunk_statistics->row_count;
      const auto right_selectivity = left_input_chunk_statistics->row_count;

      const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(left_input_chunk_statistics->row_count *
                                                                              right_input_chunk_statistics->row_count);
      output_chunk_statistics->segment_statistics.reserve(left_input_chunk_statistics->segment_statistics.size() +
                                                          right_input_chunk_statistics->segment_statistics.size());

      /**
       * Write out the SegmentStatistics
       */
      for (auto column_id = ColumnID{0}; column_id < left_input_chunk_statistics->segment_statistics.size();
           ++column_id) {
        const auto& left_segment_statistics = left_input_chunk_statistics->segment_statistics[column_id];
        output_chunk_statistics->segment_statistics.emplace_back(
            left_segment_statistics->scale_with_selectivity(left_selectivity));
      }

      for (auto column_id = ColumnID{0}; column_id < right_input_chunk_statistics->segment_statistics.size();
           ++column_id) {
        const auto& right_segment_statistics = right_input_chunk_statistics->segment_statistics[column_id];
        output_chunk_statistics->segment_statistics.emplace_back(
            right_segment_statistics->scale_with_selectivity(right_selectivity));
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }
  }

  return output_table_statistics;
}

template <typename T>
std::shared_ptr<AbstractHistogram<T>> CardinalityEstimator::get_best_available_histogram(
    const SegmentStatistics2<T>& segment_statistics) {
  if (segment_statistics.equal_distinct_count_histogram) {
    return segment_statistics.equal_distinct_count_histogram;
  } else if (segment_statistics.equal_width_histogram) {
    return segment_statistics.equal_width_histogram;
  } else if (segment_statistics.generic_histogram) {
    return segment_statistics.generic_histogram;
  } else if (segment_statistics.single_bin_histogram) {
    return segment_statistics.single_bin_histogram;
  } else {
    return nullptr;
  }
}

}  // namespace opossum
