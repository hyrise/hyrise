#include "cardinality_estimator.hpp"

#include <memory>

#include "chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "chunk_statistics/histograms/equal_width_histogram.hpp"
#include "chunk_statistics/histograms/generic_histogram.hpp"
#include "chunk_statistics/histograms/single_bin_histogram.hpp"
#include "chunk_statistics2.hpp"
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
#include "resolve_type.hpp"
#include "segment_statistics2.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_statistics2.hpp"
#include "utils/assert.hpp"

namespace opossum {

Cardinality CardinalityEstimator::estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  return estimate_statistics(lqp)->row_count();
}

std::shared_ptr<TableStatistics2> CardinalityEstimator::estimate_statistics(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  if (const auto alias_node = std::dynamic_pointer_cast<AliasNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input());
    const auto output_table_statistics = std::make_shared<TableStatistics2>();
    output_table_statistics->chunk_statistics.reserve(input_table_statistics->chunk_statistics.size());

    for (const auto& input_chunk_statistics : input_table_statistics->chunk_statistics) {
      const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(input_chunk_statistics->row_count);
      output_chunk_statistics->segment_statistics.reserve(alias_node->column_expressions().size());

      for (const auto& expression : alias_node->column_expressions()) {
        const auto input_column_id = alias_node->left_input()->get_column_id(*expression);
        output_chunk_statistics->segment_statistics.emplace_back(input_chunk_statistics->segment_statistics[input_column_id]);
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }

    return output_table_statistics;
  }

  if (const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input());
    const auto output_table_statistics = std::make_shared<TableStatistics2>();
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
            output_chunk_statistics->segment_statistics.emplace_back(std::make_shared<SegmentStatistics2<ColumnDataType>>());
          });
        }
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }

    return output_table_statistics;
  }

  if (const auto aggregate_node = std::dynamic_pointer_cast<AggregateNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input());
    const auto output_table_statistics = std::make_shared<TableStatistics2>();
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
            output_chunk_statistics->segment_statistics.emplace_back(std::make_shared<SegmentStatistics2<ColumnDataType>>());
          });
        }
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }

    return output_table_statistics;
  }

  if (const auto validate_node = std::dynamic_pointer_cast<ValidateNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input());
    const auto output_table_statistics = std::make_shared<TableStatistics2>();
    output_table_statistics->chunk_statistics.reserve(input_table_statistics->chunk_statistics.size());

    for (const auto& input_chunk_statistics : input_table_statistics->chunk_statistics) {
      if (input_chunk_statistics->row_count == 0) {
        // No need to write out statistics for Chunks without rows
        continue;
      }

      const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(input_chunk_statistics->row_count - input_chunk_statistics->approx_invalid_row_count);
      output_chunk_statistics->segment_statistics.reserve(input_chunk_statistics->segment_statistics.size());

      const auto selectivity =  (input_chunk_statistics->row_count - input_chunk_statistics->approx_invalid_row_count) / input_chunk_statistics->row_count;

      for (const auto& segment_statistics : input_chunk_statistics->segment_statistics) {
        output_chunk_statistics->segment_statistics.emplace_back(segment_statistics->scale_with_selectivity(selectivity));
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }

    return output_table_statistics;
  }

  if (std::dynamic_pointer_cast<SortNode>(lqp)) {
    return estimate_statistics(lqp->left_input());
  }

  if (const auto mock_node = std::dynamic_pointer_cast<MockNode>(lqp)) {
    Assert(mock_node->table_statistics2(), "");
    return mock_node->table_statistics2();
  }

  if (const auto stored_table_node = std::dynamic_pointer_cast<StoredTableNode>(lqp)) {
    const auto table = StorageManager::get().get_table(stored_table_node->table_name);
    Assert(table->table_statistics2(), "");
    return table->table_statistics2();
  }

  if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(lqp)) {
    const auto input_table_statistics = estimate_statistics(lqp->left_input());
    const auto output_table_statistics = std::make_shared<TableStatistics2>();

    const auto operator_scan_predicates =
        OperatorScanPredicate::from_expression(*predicate_node->predicate, *predicate_node);
    Assert(operator_scan_predicates, "NYI");

    for (auto chunk_id = ChunkID{0}; chunk_id < input_table_statistics->chunk_statistics.size(); ++chunk_id) {
      const auto input_chunk_statistics = input_table_statistics->chunk_statistics[chunk_id];
      auto output_chunk_statistics = input_chunk_statistics;

      for (const auto& operator_scan_predicate : *operator_scan_predicates) {
        const auto predicate_input_chunk_statistics = output_chunk_statistics;
        const auto predicate_output_chunk_statistics = std::make_shared<ChunkStatistics2>();
        predicate_output_chunk_statistics->segment_statistics.resize(
            predicate_input_chunk_statistics->segment_statistics.size());

        auto predicate_chunk_selectivity = Selectivity{1};

        /**
         * Manipulate statistics of column that we scan on
         */
        const auto base_segment_statistics =
            output_chunk_statistics->segment_statistics.at(operator_scan_predicate.column_id);

        const auto data_type = predicate_node->column_expressions().at(operator_scan_predicate.column_id)->data_type();

        resolve_data_type(data_type, [&](const auto data_type_t) {
          using SegmentDataType = typename decltype(data_type_t)::type;

          const auto segment_statistics =
              std::static_pointer_cast<SegmentStatistics2<SegmentDataType>>(base_segment_statistics);

          auto primary_scan_statistics_object = this->get_best_available_histogram(*segment_statistics);

          Assert(operator_scan_predicate.value.type() == typeid(AllTypeVariant),
                 "Histogram can't handle column-to-column scans right now");

          auto value2_all_type_variant = std::optional<AllTypeVariant>{};
          if (operator_scan_predicate.value2) {
            Assert(operator_scan_predicate.value2->type() == typeid(AllTypeVariant),
                   "Histogram can't handle column-to-column scans right now");
            value2_all_type_variant = boost::get<AllTypeVariant>(*operator_scan_predicate.value2);
          }

          const auto sliced_statistics_object = primary_scan_statistics_object->slice_with_predicate(
                  operator_scan_predicate.predicate_condition,
                  boost::get<AllTypeVariant>(operator_scan_predicate.value), value2_all_type_variant);

          const auto cardinality_estimate = primary_scan_statistics_object->estimate_cardinality(
                  operator_scan_predicate.predicate_condition,
                  boost::get<AllTypeVariant>(operator_scan_predicate.value), value2_all_type_variant);

          if (predicate_input_chunk_statistics->row_count == 0) {
            predicate_chunk_selectivity = Selectivity{0};
          } else if (cardinality_estimate.estimate_type == EstimateType::MatchesNone) {

          } else {
            predicate_chunk_selectivity = cardinality_estimate.cardinality / predicate_input_chunk_statistics->row_count;
          }

          const auto output_segment_statistics = std::make_shared<SegmentStatistics2<SegmentDataType>>();
          output_segment_statistics->set_statistics_object(sliced_statistics_object);

          predicate_output_chunk_statistics->segment_statistics[operator_scan_predicate.column_id] =
              output_segment_statistics;
        });

        /**
         * Manipulate statistics of all columns that we DIDN'T scan on with this predicate
         */
        for (auto column_id = ColumnID{0}; column_id < predicate_input_chunk_statistics->segment_statistics.size();
             ++column_id) {
          if (column_id == operator_scan_predicate.column_id) continue;

          predicate_output_chunk_statistics->segment_statistics[column_id] =
              predicate_input_chunk_statistics->segment_statistics[column_id]->scale_with_selectivity(
                  predicate_chunk_selectivity);
        }

        /**
         * Adjust ChunkStatistics row_count
         */
        predicate_output_chunk_statistics->row_count =
            predicate_input_chunk_statistics->row_count * predicate_chunk_selectivity;

        output_chunk_statistics = predicate_output_chunk_statistics;
      }

      output_table_statistics->chunk_statistics.emplace_back(output_chunk_statistics);
    }

    return output_table_statistics;
  }

  if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(lqp)) {
    switch (join_node->join_mode) {
      case JoinMode::Inner: {
        const auto left_input_table_statistics = estimate_statistics(lqp->left_input());
        const auto right_input_table_statistics = estimate_statistics(lqp->right_input());
        const auto output_table_statistics = std::make_shared<TableStatistics2>();
        output_table_statistics->chunk_statistics.reserve(left_input_table_statistics->chunk_statistics.size() *
                                                          right_input_table_statistics->chunk_statistics.size());

        const auto operator_join_predicate = OperatorJoinPredicate::from_join_node(*join_node);
        Assert(operator_join_predicate, "");
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

          for (const auto& left_input_chunk_statistics : left_input_table_statistics->chunk_statistics) {
            for (const auto& right_input_chunk_statistics : right_input_table_statistics->chunk_statistics) {
              const auto left_input_segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
                  left_input_chunk_statistics->segment_statistics[left_column_id]);
              const auto right_input_segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
                  right_input_chunk_statistics->segment_statistics[right_column_id]);

              const auto left_histogram = this->get_best_available_histogram(*left_input_segment_statistics);
              const auto right_histogram = this->get_best_available_histogram(*right_input_segment_statistics);
              Assert(left_histogram && right_histogram, "NYI");

              const auto unified_left_histogram = left_histogram->split_at_bin_edges(right_histogram->bin_edges());
              const auto unified_right_histogram = right_histogram->split_at_bin_edges(left_histogram->bin_edges());

              const auto join_histogram = estimate_histogram_of_inner_equi_join_with_arithmetic_histograms(
                  unified_left_histogram, unified_right_histogram);

              const auto cardinality = join_histogram->total_count();

              const auto output_chunk_statistics = std::make_shared<ChunkStatistics2>(cardinality);
              output_chunk_statistics->segment_statistics.reserve(
                  left_input_chunk_statistics->segment_statistics.size() +
                  right_input_chunk_statistics->segment_statistics.size());

              const auto left_selectivity = cardinality / left_input_chunk_statistics->row_count;
              const auto right_selectivity = cardinality / right_input_chunk_statistics->row_count;

              for (auto column_id = ColumnID{0}; column_id < left_input_chunk_statistics->segment_statistics.size();
                   ++column_id) {
                if (column_id == left_column_id) {
                  const auto segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
                  segment_statistics->set_statistics_object(join_histogram);
                  output_chunk_statistics->segment_statistics.emplace_back(segment_statistics);
                } else {
                  const auto& left_segment_statistics = left_input_chunk_statistics->segment_statistics[column_id];
                  output_chunk_statistics->segment_statistics.emplace_back(
                      left_segment_statistics->scale_with_selectivity(left_selectivity));
                }
              }

              for (auto column_id = ColumnID{0}; column_id < right_input_chunk_statistics->segment_statistics.size();
                   ++column_id) {
                if (column_id == right_column_id) {
                  const auto segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
                  segment_statistics->set_statistics_object(join_histogram);
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

        return output_table_statistics;
      } break;

      default:
        Fail("JoinMode not implemented");
    }
  }

  Fail("NYI");
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
    Fail("No statistics object available");
  }
}

}  // namespace opossum
