#include "cardinality_estimation_join.hpp"

#include <iostream>

#include "operators/operator_join_predicate.hpp"
#include "resolve_type.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/generic_histogram_builder.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/vertical_statistics_slice.hpp"
#include "statistics/table_cardinality_estimation_statistics.hpp"
#include "statistics/horizontal_statistics_slice.hpp"

namespace opossum {

template <typename T>
std::pair<HistogramCountType, HistogramCountType> estimate_inner_equi_join_of_histogram_bins(T left_height,
                                                                                             T left_distinct_count,
                                                                                             T right_height,
                                                                                             T right_distinct_count) {
  // Range with more distinct values should be on the left side to keep the algorithm below simple
  if (left_distinct_count < right_distinct_count) {
    return estimate_inner_equi_join_of_histogram_bins(right_height, right_distinct_count, left_height,
                                                      left_distinct_count);
  }

  if (left_distinct_count == 0 || right_distinct_count == 0) {
    return {0.0f, 0.0f};
  }

  // Perform a basic principle-of-inclusion join estimation

  const auto right_density = right_height / right_distinct_count;

  const auto match_ratio = right_distinct_count / left_distinct_count;
  const auto match_count = left_height * match_ratio * right_density;

  return {match_count, right_distinct_count};
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> estimate_histogram_of_inner_equi_join_with_bin_adjusted_histograms(
    const std::shared_ptr<AbstractHistogram<T>>& left_histogram,
    const std::shared_ptr<AbstractHistogram<T>>& right_histogram) {
  auto left_idx = BinID{0};
  auto right_idx = BinID{0};
  auto left_bin_count = left_histogram->bin_count();
  auto right_bin_count = right_histogram->bin_count();

  GenericHistogramBuilder<T> builder;

  for (; left_idx < left_bin_count && right_idx < right_bin_count;) {
    const auto left_min = left_histogram->bin_minimum(left_idx);
    const auto right_min = right_histogram->bin_minimum(right_idx);

    if (left_min < right_min) {
      ++left_idx;
      continue;
    }

    if (right_min < left_min) {
      ++right_idx;
      continue;
    }

    DebugAssert(left_histogram->bin_maximum(left_idx) == right_histogram->bin_maximum(right_idx),
                "Histogram bin boundaries do not match");

    const auto [height, distinct_count] = estimate_inner_equi_join_of_histogram_bins(
        left_histogram->bin_height(left_idx), left_histogram->bin_distinct_count(left_idx),
        right_histogram->bin_height(right_idx), right_histogram->bin_distinct_count(right_idx));
    if (height != 0) {
      builder.add_bin(left_min, left_histogram->bin_maximum(left_idx), height, distinct_count);
    }

    ++left_idx;
    ++right_idx;
  }

  if (builder.empty()) {
    return nullptr;
  }

  return builder.build();
}

std::shared_ptr<TableCardinalityEstimationStatistics> cardinality_estimation_inner_equi_join(
    const ColumnID left_column_id, const ColumnID right_column_id,
    const std::shared_ptr<TableCardinalityEstimationStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableCardinalityEstimationStatistics>& right_input_table_statistics) {
  // Concatenate left and right column data types
  auto column_data_types = left_input_table_statistics->column_data_types;
  column_data_types.reserve(left_input_table_statistics->column_count() + right_input_table_statistics->column_count());
  column_data_types.insert(column_data_types.end(), right_input_table_statistics->column_data_types.begin(),
                           right_input_table_statistics->column_data_types.end());

  const auto output_table_statistics = std::make_shared<TableCardinalityEstimationStatistics>(column_data_types);

  const auto& left_statistics_slices = left_input_table_statistics->cardinality_estimation_slices;
  const auto& right_statistics_slices = right_input_table_statistics->cardinality_estimation_slices;

  if (left_statistics_slices.empty() || right_statistics_slices.empty()) {
    return output_table_statistics;
  }

  // TODO(anybody) For many Chunks on both sides this nested loop further down will be inefficient.
  //               Consider approaches to merge statistic objects on each side.
  if (left_statistics_slices.size() > 1 || right_statistics_slices.size() > 1) {
    PerformanceWarning("CardinalityEstimation of join is performed on non-compact ChunkStatisticsSet.");
  }

  const auto left_data_type = left_input_table_statistics->column_data_types[left_column_id];
  const auto right_data_type = right_input_table_statistics->column_data_types[right_column_id];

  // TODO(anybody) - Implement join estimation for differing column data types
  //               - Implement join estimation for String columns
  if (left_data_type != right_data_type || left_data_type == DataType::String) {
    return cardinality_estimation_cross_join(left_input_table_statistics, right_input_table_statistics);
  }

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    for (const auto& left_input_statistics_slice : left_statistics_slices) {
      for (const auto& right_input_statistics_slice : right_statistics_slices) {
        const auto left_input_vertical_slices = std::dynamic_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(
            left_input_statistics_slice->vertical_slices[left_column_id]);
        const auto right_input_vertical_slices = std::dynamic_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(
            right_input_statistics_slice->vertical_slices[right_column_id]);

        auto cardinality = Cardinality{0};
        auto join_column_histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

        auto left_histogram = left_input_vertical_slices->histogram;
        auto right_histogram = right_input_vertical_slices->histogram;

        if (left_histogram && right_histogram) {
          auto unified_left_histogram = left_histogram->split_at_bin_bounds(right_histogram->bin_bounds());
          auto unified_right_histogram = right_histogram->split_at_bin_bounds(left_histogram->bin_bounds());

          Assert(unified_left_histogram && unified_right_histogram, "Creating unified histograms should not fail");

          join_column_histogram = estimate_histogram_of_inner_equi_join_with_bin_adjusted_histograms(
              unified_left_histogram, unified_right_histogram);

          if (!join_column_histogram) {
            // Not matches in this Chunk-pair
            continue;
          }

          cardinality = join_column_histogram->total_count();
        } else {
          // TODO(anybody) If creating the unified histograms failed, use some other algorithm to estimate the Join
          cardinality = left_input_statistics_slice->row_count * right_input_statistics_slice->row_count;
        }

        const auto output_statistics_slice = std::make_shared<HorizontalStatisticsSlice>(cardinality);
        output_statistics_slice->vertical_slices.reserve(left_input_statistics_slice->vertical_slices.size() +
                                                            right_input_statistics_slice->vertical_slices.size());

        const auto left_selectivity =
            left_input_statistics_slice->row_count > 0 ? cardinality / left_input_statistics_slice->row_count : 0.0f;
        const auto right_selectivity =
            right_input_statistics_slice->row_count > 0 ? cardinality / right_input_statistics_slice->row_count : 0.0f;

        /**
         * Write out the SegmentStatistics
         */
        for (auto column_id = ColumnID{0}; column_id < left_input_statistics_slice->vertical_slices.size();
             ++column_id) {
          if (join_column_histogram && column_id == left_column_id) {
            const auto vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();
            vertical_slices->set_statistics_object(join_column_histogram);
            output_statistics_slice->vertical_slices.emplace_back(vertical_slices);
          } else {
            const auto& left_vertical_slices = left_input_statistics_slice->vertical_slices[column_id];
            output_statistics_slice->vertical_slices.emplace_back(left_vertical_slices->scaled(left_selectivity));
          }
        }

        for (auto column_id = ColumnID{0}; column_id < right_input_statistics_slice->vertical_slices.size();
             ++column_id) {
          if (join_column_histogram && column_id == right_column_id) {
            const auto vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();
            vertical_slices->set_statistics_object(join_column_histogram);
            output_statistics_slice->vertical_slices.emplace_back(vertical_slices);
          } else {
            const auto& right_vertical_slices = right_input_statistics_slice->vertical_slices[column_id];
            output_statistics_slice->vertical_slices.emplace_back(
                right_vertical_slices->scaled(right_selectivity));
          }
        }

        output_table_statistics->cardinality_estimation_slices.emplace_back(output_statistics_slice);
      }
    }
  });

  return output_table_statistics;
}

std::shared_ptr<TableCardinalityEstimationStatistics> cardinality_estimation_inner_join(
    const OperatorJoinPredicate& join_predicate, const std::shared_ptr<TableCardinalityEstimationStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableCardinalityEstimationStatistics>& right_input_table_statistics) {
  if (join_predicate.predicate_condition == PredicateCondition::Equals) {
    return cardinality_estimation_inner_equi_join(join_predicate.column_ids.first, join_predicate.column_ids.second,
                                                  left_input_table_statistics, right_input_table_statistics);
  } else {
    // TODO(anybody) Implement estimation for non-equi joins
    return cardinality_estimation_cross_join(left_input_table_statistics, right_input_table_statistics);
  }
}

std::shared_ptr<TableCardinalityEstimationStatistics> cardinality_estimation_predicated_join(
    const JoinMode join_mode, const OperatorJoinPredicate& join_predicate,
    const std::shared_ptr<TableCardinalityEstimationStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableCardinalityEstimationStatistics>& right_input_table_statistics) {
  switch (join_mode) {
    case JoinMode::Semi:
    case JoinMode::Anti:
      // TODO(anybody) Implement estimation of Semi/Anti joins
      return left_input_table_statistics;

    // TODO(anybody) For now, handle outer joins just as inner joins
    case JoinMode::Left:
    case JoinMode::Right:
    case JoinMode::Outer:
    case JoinMode::Inner:
      return cardinality_estimation_inner_join(join_predicate, left_input_table_statistics,
                                               right_input_table_statistics);

    case JoinMode::Cross:
      // Should have been forwarded to cardinality_estimation_cross_join
      Fail("Cross join is not a predicated join");
  }
}

std::shared_ptr<TableCardinalityEstimationStatistics> cardinality_estimation_cross_join(
    const std::shared_ptr<TableCardinalityEstimationStatistics>& left_input_table_statistics,
    const std::shared_ptr<TableCardinalityEstimationStatistics>& right_input_table_statistics) {
  // Concatenate left and right column data types
  auto column_data_types = left_input_table_statistics->column_data_types;
  column_data_types.reserve(left_input_table_statistics->column_count() + right_input_table_statistics->column_count());
  column_data_types.insert(column_data_types.end(), right_input_table_statistics->column_data_types.begin(),
                           right_input_table_statistics->column_data_types.end());

  const auto output_table_statistics = std::make_shared<TableCardinalityEstimationStatistics>(column_data_types);

  const auto& left_statistics_slices = left_input_table_statistics->cardinality_estimation_slices;
  const auto& right_statistics_slices = right_input_table_statistics->cardinality_estimation_slices;

  if (left_statistics_slices.empty() || right_statistics_slices.empty()) {
    return output_table_statistics;
  }

  // TODO(anybody) For many Chunks on both sides this nested loop further down will be inefficient.
  //               Consider approaches to merge statistic objects on each side.
  if (left_statistics_slices.size() > 1 || right_statistics_slices.size() > 1) {
    PerformanceWarning("CardinalityEstimation of join is performed on non-compact ChunkStatisticsSet.");
  }

  for (const auto& left_input_statistics_slice : left_statistics_slices) {
    for (const auto& right_input_statistics_slice : right_statistics_slices) {
      const auto left_selectivity = right_input_statistics_slice->row_count;
      const auto right_selectivity = left_input_statistics_slice->row_count;

      const auto output_statistics_slice = std::make_shared<HorizontalStatisticsSlice>(
          left_input_statistics_slice->row_count * right_input_statistics_slice->row_count);
      output_statistics_slice->vertical_slices.reserve(left_input_statistics_slice->vertical_slices.size() +
                                                          right_input_statistics_slice->vertical_slices.size());

      /**
       * Write out the SegmentStatistics
       */
      for (auto column_id = ColumnID{0}; column_id < left_input_statistics_slice->vertical_slices.size();
           ++column_id) {
        const auto& left_vertical_slices = left_input_statistics_slice->vertical_slices[column_id];
        output_statistics_slice->vertical_slices.emplace_back(left_vertical_slices->scaled(left_selectivity));
      }

      for (auto column_id = ColumnID{0}; column_id < right_input_statistics_slice->vertical_slices.size();
           ++column_id) {
        const auto& right_vertical_slices = right_input_statistics_slice->vertical_slices[column_id];
        output_statistics_slice->vertical_slices.emplace_back(right_vertical_slices->scaled(right_selectivity));
      }

      output_table_statistics->cardinality_estimation_slices.emplace_back(output_statistics_slice);
    }
  }

  return output_table_statistics;
}

}  // namespace opossum