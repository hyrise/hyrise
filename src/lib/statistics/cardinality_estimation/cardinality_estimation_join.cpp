#include "cardinality_estimation_join.hpp"

#include <iostream>

#include "operators/operator_join_predicate.hpp"
#include "resolve_type.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/generic_histogram_builder.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/horizontal_statistics_slice.hpp"
#include "statistics/table_cardinality_estimation_statistics.hpp"
#include "statistics/vertical_statistics_slice.hpp"

namespace opossum {

namespace cardinality_estimation {

std::pair<HistogramCountType, HistogramCountType> bins_inner_equi_join(const float left_height,
                                                                       const float left_distinct_count,
                                                                       const float right_height,
                                                                       const float right_distinct_count) {
  // Range with more distinct values should be on the left side to keep the algorithm below simple
  if (left_distinct_count < right_distinct_count) {
    return bins_inner_equi_join(right_height, right_distinct_count, left_height, left_distinct_count);
  }

  // Early out to avoid division by zero below
  if (left_distinct_count == 0 || right_distinct_count == 0) {
    return {0.0f, 0.0f};
  }

  // Perform a basic principle-of-inclusion join estimation

  // Each distinct value on the right side occurs `right_density` times.
  // E.g., if right_height == 10 and right_distinct_count == 2, then each distinct value occurs 5 times.
  const auto right_density = right_height / right_distinct_count;

  // "principle-of-inclusion" means every distinct value on the right side finds a match left. `left_match_ratio` is the
  // ratio of distinct values on the left side that find a match on the right side
  // E.g., if right_distinct_count == 10 and left_distinct_count == 30, then one third of the rows from the
  // left side will find a match.
  const auto left_match_ratio = right_distinct_count / left_distinct_count;

  // `left_height * left_match_ratio` is the number of rows on the left side that will find matches. `right_density` is
  // the number of matches each row on the left side finds. Multiply them to get the number of resulting matches.
  const auto match_count = left_height * left_match_ratio * right_density;

  return {match_count, right_distinct_count};
}

template <typename T>
std::shared_ptr<GenericHistogram<T>> histograms_inner_equi_join(const AbstractHistogram<T>& left_histogram,
                                                                const AbstractHistogram<T>& right_histogram) {
  /**
   * left_histogram and right_histogram are turned into "unified" histograms by `split_at_bin_bounds`, meaning that
   * their bins are split until their bin boundaries match.
   * E.g., if left_histogram has a single bin [1, 10] and right histogram has a single bin [5, 20] then
   * unified_left_histogram == {[1, 4], [5, 10]}
   * unified_right_histogram == {[5, 10], [11, 20]}
   * The estimation is performed on overlapping bins only, e.g., only the two bins [5, 10] will produce matches.
   */

  auto unified_left_histogram = left_histogram.split_at_bin_bounds(right_histogram.bin_bounds());
  auto unified_right_histogram = right_histogram.split_at_bin_bounds(left_histogram.bin_bounds());

  Assert(unified_left_histogram && unified_right_histogram, "Creating unified histograms should not fail");

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

    // Overlapping bins found, estimate the join for these bin's range
    const auto [height, distinct_count] = bins_inner_equi_join(
        unified_left_histogram->bin_height(left_idx), unified_left_histogram->bin_distinct_count(left_idx),
        unified_right_histogram->bin_height(right_idx), unified_right_histogram->bin_distinct_count(right_idx));

    if (height > 0) {
      builder.add_bin(left_min, unified_left_histogram->bin_maximum(left_idx), height, distinct_count);
    }

    ++left_idx;
    ++right_idx;
  }

  if (builder.empty()) {
    return nullptr;
  }

  return builder.build();
}

std::shared_ptr<TableCardinalityEstimationStatistics> inner_equi_join(
    const ColumnID left_column_id, const ColumnID right_column_id,
    const TableCardinalityEstimationStatistics& left_input_table_statistics,
    const TableCardinalityEstimationStatistics& right_input_table_statistics) {

  // Concatenate left and right column data types for the output TableCardinalityEstimationStatistics
  auto column_data_types = left_input_table_statistics.column_data_types;
  column_data_types.insert(column_data_types.end(), right_input_table_statistics.column_data_types.begin(),
                           right_input_table_statistics.column_data_types.end());

  const auto output_table_statistics = std::make_shared<TableCardinalityEstimationStatistics>(column_data_types);

  const auto& left_horizontal_slices = left_input_table_statistics.horizontal_slices;
  const auto& right_horizontal_slices = right_input_table_statistics.horizontal_slices;

  if (left_horizontal_slices.empty() || right_horizontal_slices.empty()) {
    // No slices on either side? Return an empty table statistics object
    return output_table_statistics;
  }

  // TODO(anybody) For many Chunks on both sides this nested loop further down will be inefficient.
  //               Consider approaches to merge statistic objects on each side.
  if (left_horizontal_slices.size() > 1 || right_horizontal_slices.size() > 1) {
    PerformanceWarning("CardinalityEstimation of join is performed on non-compact ChunkStatisticsSet.");
  }

  const auto left_data_type = left_input_table_statistics.column_data_types[left_column_id];
  const auto right_data_type = right_input_table_statistics.column_data_types[right_column_id];

  // TODO(anybody) - Implement join estimation for differing column data types
  //               - Implement join estimation for String columns
  if (left_data_type != right_data_type || left_data_type == DataType::String) {
    return cross_join(left_input_table_statistics, right_input_table_statistics);
  }

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    /**
     * Estimate the join of each horizontal statistics slice on the left side with each horizontal slice on the right
     * side
     */

    for (const auto& left_input_horizontal_slice : left_horizontal_slices) {
      for (const auto& right_input_horizontal_slice : right_horizontal_slices) {
        const auto left_input_vertical_slices = std::dynamic_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(
            left_input_horizontal_slice->vertical_slices[left_column_id]);
        const auto right_input_vertical_slices = std::dynamic_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(
            right_input_horizontal_slice->vertical_slices[right_column_id]);

        auto cardinality = Cardinality{0};
        auto join_column_histogram = std::shared_ptr<AbstractHistogram<ColumnDataType>>{};

        auto left_histogram = left_input_vertical_slices->histogram;
        auto right_histogram = right_input_vertical_slices->histogram;

        if (left_histogram && right_histogram) {
          join_column_histogram = histograms_inner_equi_join(*left_histogram, *right_histogram);

          if (!join_column_histogram) {
            // Not matches in this Chunk-pair
            continue;
          }

          cardinality = join_column_histogram->total_count();
        } else {
          // TODO(anybody) If there aren't histograms on both sides, use some other algorithm to estimate the Join
          cardinality = left_input_horizontal_slice->row_count * right_input_horizontal_slice->row_count;
        }

        const auto output_horizontal_slice = std::make_shared<HorizontalStatisticsSlice>(cardinality);
        output_horizontal_slice->vertical_slices.reserve(left_input_horizontal_slice->vertical_slices.size() +
                                                         right_input_horizontal_slice->vertical_slices.size());

        const auto left_selectivity =
            left_input_horizontal_slice->row_count > 0 ? cardinality / left_input_horizontal_slice->row_count : 0.0f;
        const auto right_selectivity =
            right_input_horizontal_slice->row_count > 0 ? cardinality / right_input_horizontal_slice->row_count : 0.0f;

        /**
         * Write out the VerticalStatisticsSlices of all columns. With no correlation info available, simply scale all
         * vertical slices that didn't participate the join predicate
         */
        for (auto column_id = ColumnID{0}; column_id < left_input_horizontal_slice->vertical_slices.size();
             ++column_id) {
          if (join_column_histogram && column_id == left_column_id) {
            const auto vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();
            vertical_slices->set_statistics_object(join_column_histogram);
            output_horizontal_slice->vertical_slices.emplace_back(vertical_slices);
          } else {
            const auto& left_vertical_slices = left_input_horizontal_slice->vertical_slices[column_id];
            output_horizontal_slice->vertical_slices.emplace_back(left_vertical_slices->scaled(left_selectivity));
          }
        }

        for (auto column_id = ColumnID{0}; column_id < right_input_horizontal_slice->vertical_slices.size();
             ++column_id) {
          if (join_column_histogram && column_id == right_column_id) {
            const auto vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();
            vertical_slices->set_statistics_object(join_column_histogram);
            output_horizontal_slice->vertical_slices.emplace_back(vertical_slices);
          } else {
            const auto& right_vertical_slices = right_input_horizontal_slice->vertical_slices[column_id];
            output_horizontal_slice->vertical_slices.emplace_back(right_vertical_slices->scaled(right_selectivity));
          }
        }

        output_table_statistics->horizontal_slices.emplace_back(output_horizontal_slice);
      }
    }
  });

  return output_table_statistics;
}

std::shared_ptr<TableCardinalityEstimationStatistics> cross_join(
    const TableCardinalityEstimationStatistics& left_input_table_statistics,
    const TableCardinalityEstimationStatistics& right_input_table_statistics) {
  // Concatenate left and right column data types
  auto column_data_types = left_input_table_statistics.column_data_types;
  column_data_types.insert(column_data_types.end(), right_input_table_statistics.column_data_types.begin(),
                           right_input_table_statistics.column_data_types.end());

  const auto output_table_statistics = std::make_shared<TableCardinalityEstimationStatistics>(column_data_types);

  const auto& left_horizontal_slices = left_input_table_statistics.horizontal_slices;
  const auto& right_horizontal_slices = right_input_table_statistics.horizontal_slices;

  if (left_horizontal_slices.empty() || right_horizontal_slices.empty()) {
    return output_table_statistics;
  }

  // TODO(anybody) For many Chunks on both sides this nested loop further down will be inefficient.
  //               Consider approaches to merge statistic objects on each side.
  if (left_horizontal_slices.size() > 1 || right_horizontal_slices.size() > 1) {
    PerformanceWarning("CardinalityEstimation of join is performed on non-compact ChunkStatisticsSet.");
  }

  for (const auto& left_input_horizontal_slice : left_horizontal_slices) {
    for (const auto& right_input_horizontal_slice : right_horizontal_slices) {
      const auto left_selectivity = right_input_horizontal_slice->row_count;
      const auto right_selectivity = left_input_horizontal_slice->row_count;

      const auto output_horizontal_slice = std::make_shared<HorizontalStatisticsSlice>(
          left_input_horizontal_slice->row_count * right_input_horizontal_slice->row_count);
      output_horizontal_slice->vertical_slices.reserve(left_input_horizontal_slice->vertical_slices.size() +
                                                       right_input_horizontal_slice->vertical_slices.size());

      /**
       * Write out the VerticalStatisticsSlices, scaled up.
       */
      for (auto column_id = ColumnID{0}; column_id < left_input_horizontal_slice->vertical_slices.size(); ++column_id) {
        const auto& left_vertical_slice = left_input_horizontal_slice->vertical_slices[column_id];
        output_horizontal_slice->vertical_slices.emplace_back(left_vertical_slice->scaled(left_selectivity));
      }

      for (auto column_id = ColumnID{0}; column_id < right_input_horizontal_slice->vertical_slices.size();
           ++column_id) {
        const auto& right_vertical_slice = right_input_horizontal_slice->vertical_slices[column_id];
        output_horizontal_slice->vertical_slices.emplace_back(right_vertical_slice->scaled(right_selectivity));
      }

      output_table_statistics->horizontal_slices.emplace_back(output_horizontal_slice);
    }
  }

  return output_table_statistics;
}

}  // namespace cardinality_estimation

}  // namespace opossum
