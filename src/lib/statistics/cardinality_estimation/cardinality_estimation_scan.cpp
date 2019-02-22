#include "cardinality_estimation_scan.hpp"

#include "operators/operator_scan_predicate.hpp"
#include "resolve_type.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/generic_histogram_builder.hpp"
#include "statistics/horizontal_statistics_slice.hpp"
#include "statistics/vertical_statistics_slice.hpp"

namespace opossum {

template <typename T>
std::shared_ptr<GenericHistogram<T>> estimate_column_to_column_equi_scan(const AbstractHistogram<T>& left_histogram,
                                                                         const AbstractHistogram<T>& right_histogram) {
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

template <typename T>
std::optional<float> estimate_null_value_ratio_of_segment(
    const std::shared_ptr<HorizontalStatisticsSlice>& chunk_statistics,
    const std::shared_ptr<VerticalStatisticsSlice<T>>& vertical_slices) {
  if (vertical_slices->null_value_ratio) {
    return vertical_slices->null_value_ratio->null_value_ratio;
  }

  if (vertical_slices->histogram) {
    if (chunk_statistics->row_count != 0) {
      return 1.0f - (static_cast<float>(vertical_slices->histogram->total_count()) / chunk_statistics->row_count);
    }
  }

  return std::nullopt;
}

std::shared_ptr<HorizontalStatisticsSlice> cardinality_estimation_scan_slice(
    const std::shared_ptr<HorizontalStatisticsSlice>& input_statistics_slice, const OperatorScanPredicate& predicate) {
  auto output_statistics_slice = std::make_shared<HorizontalStatisticsSlice>();
  output_statistics_slice->vertical_slices.resize(input_statistics_slice->vertical_slices.size());

  auto selectivity = Selectivity{1};

  const auto left_column_id = predicate.column_id;
  auto right_column_id = std::optional<ColumnID>{};

  /**
   * Manipulate statistics of column that we scan on
   */
  const auto base_vertical_slice = input_statistics_slice->vertical_slices.at(left_column_id);

  const auto left_data_type = input_statistics_slice->vertical_slices[left_column_id]->data_type;

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto input_vertical_slices =
        std::static_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(base_vertical_slice);

    if (predicate.predicate_condition == PredicateCondition::IsNull) {
      const auto null_value_ratio = estimate_null_value_ratio_of_segment(input_statistics_slice, input_vertical_slices);

      if (null_value_ratio) {
        selectivity = *null_value_ratio;

        // All that remains of the column we scanned on are NULL values
        const auto output_vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();
        output_vertical_slices->set_statistics_object(std::make_shared<NullValueRatio>(1.0f));
        output_statistics_slice->vertical_slices[left_column_id] = output_vertical_slices;
      } else {
        // If have no null-value ratio available, assume a selectivity of 1, for both IS NULL and IS NOT NULL
        selectivity = 1.0f;
      }
    } else if (predicate.predicate_condition == PredicateCondition::IsNotNull) {
      const auto null_value_ratio = estimate_null_value_ratio_of_segment(input_statistics_slice, input_vertical_slices);

      if (null_value_ratio) {
        selectivity = 1.0f - *null_value_ratio;

        // No NULL values remain in the column we scanned on
        const auto output_vertical_slices = input_vertical_slices->scaled(selectivity);
        output_vertical_slices->set_statistics_object(std::make_shared<NullValueRatio>(0.0f));
        output_statistics_slice->vertical_slices[left_column_id] = output_vertical_slices;
      } else {
        // If have no null-value ratio available, assume a selectivity of 1, for both IS NULL and IS NOT NULL
        selectivity = 1.0f;
      }
    } else {
      const auto scan_statistics_object = input_vertical_slices->histogram;
      // If there are no statistics available for this segment, assume a selectivity of 1
      if (!scan_statistics_object) {
        selectivity = 1.0f;
        return;
      }

      if (predicate.value.type() == typeid(ColumnID)) {
        right_column_id = boost::get<ColumnID>(predicate.value);

        const auto right_data_type = input_statistics_slice->vertical_slices[*right_column_id]->data_type;

        if (left_data_type != right_data_type) {
          // TODO(anybody) Cannot estimate column-vs-column scan for differing data types, yet
          selectivity = 1.0f;
          return;
        }

        if (predicate.predicate_condition != PredicateCondition::Equals) {
          // TODO(anyone) CardinalityEstimator cannot handle non-equi column-to-column scans right now
          selectivity = 1.0f;
          return;
        }

        const auto left_input_vertical_slice = std::dynamic_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(
            input_statistics_slice->vertical_slices[left_column_id]);
        const auto right_input_vertical_slice = std::dynamic_pointer_cast<VerticalStatisticsSlice<ColumnDataType>>(
            input_statistics_slice->vertical_slices[*right_column_id]);

        const auto left_histogram = left_input_vertical_slice->histogram;
        const auto right_histogram = right_input_vertical_slice->histogram;
        if (!left_histogram || !right_histogram) {
          // TODO(anyone) Can only use histograms to estimate column-to-column scans right now
          selectivity = 1.0f;
          return;
        }

        const auto bin_adjusted_left_histogram = left_histogram->split_at_bin_bounds(right_histogram->bin_bounds());
        if (!bin_adjusted_left_histogram) {
          selectivity = 1.0f;
          return;
        }

        const auto bin_adjusted_right_histogram = right_histogram->split_at_bin_bounds(left_histogram->bin_bounds());
        if (!bin_adjusted_right_histogram) {
          selectivity = 1.0f;
          return;
        }

        const auto column_to_column_histogram =
            estimate_column_to_column_equi_scan(*bin_adjusted_left_histogram, *bin_adjusted_right_histogram);
        if (!column_to_column_histogram) {
          // No matches in this Chunk estimated; prune the ChunkStatistics
          selectivity = 0.0f;
          return;
        }

        const auto cardinality = column_to_column_histogram->total_count();
        selectivity = input_statistics_slice->row_count == 0 ? 0.0f : cardinality / input_statistics_slice->row_count;

        /**
         * Write out the SegmentStatistics
         */
        const auto output_vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();
        output_vertical_slices->set_statistics_object(column_to_column_histogram);
        output_statistics_slice->vertical_slices[left_column_id] = output_vertical_slices;
        output_statistics_slice->vertical_slices[*right_column_id] = output_vertical_slices;

      } else if (predicate.value.type() == typeid(ParameterID)) {
        switch (predicate.predicate_condition) {
          case PredicateCondition::Equals: {
            const auto total_distinct_count = std::max(scan_statistics_object->total_distinct_count(), 1.0f);
            selectivity = total_distinct_count > 0 ? 1.0f / total_distinct_count : 0.0f;
          } break;

          case PredicateCondition::NotEquals: {
            const auto total_distinct_count = std::max(scan_statistics_object->total_distinct_count(), 1.0f);
            selectivity = total_distinct_count > 0 ? (total_distinct_count - 1.0f) / total_distinct_count : 0.0f;
          } break;

          case PredicateCondition::LessThan:
          case PredicateCondition::LessThanEquals:
          case PredicateCondition::GreaterThan:
          case PredicateCondition::GreaterThanEquals:
          case PredicateCondition::Between:
          case PredicateCondition::In:
          case PredicateCondition::NotIn:
          case PredicateCondition::Like:
          case PredicateCondition::NotLike:
            // Lacking better options, assume a "magic" selectivity for >, >=, <, <=, ....
            selectivity = 0.5f;
            break;

          case PredicateCondition::IsNull:
          case PredicateCondition::IsNotNull:
            Fail("IS (NOT) NULL predicates should not have a 'value' parameter.");
        }

      } else {  // value is an AllTypeVariant
        Assert(predicate.value.type() == typeid(AllTypeVariant), "Expected AllTypeVariant");

        // TODO(anybody) For (NOT) LIKE predicates that start with a wildcard, Histograms won't yield reasonable
        //               results. Assume a magic selectivity for now
        if (predicate.predicate_condition == PredicateCondition::Like) {
          selectivity = 0.1f;
          return;
        }
        if (predicate.predicate_condition == PredicateCondition::NotLike) {
          selectivity = 0.9f;
          return;
        }

        auto value2_variant = std::optional<AllTypeVariant>{};
        if (predicate.value2) {
          if (predicate.value2->type() != typeid(AllTypeVariant)) {
            selectivity = 1.0f;
            return;
          }

          value2_variant = boost::get<AllTypeVariant>(*predicate.value2);
        }

        const auto sliced_statistics_object = scan_statistics_object->sliced(
            predicate.predicate_condition, boost::get<AllTypeVariant>(predicate.value), value2_variant);

        if (!sliced_statistics_object) {
          selectivity = 0.0f;
          return;
        }

        // TODO(anybody) Simplify this block if AbstractStatisticsObject ever supports total_count()
        const auto sliced_histogram =
            std::dynamic_pointer_cast<AbstractHistogram<ColumnDataType>>(sliced_statistics_object);
        if (sliced_histogram) {
          if (input_statistics_slice->row_count == 0 || sliced_histogram->total_count() == 0.0f) {
            // No matches in this Chunk estimated; prune the ChunkStatistics
            selectivity = 0.0f;
            return;
          } else {
            selectivity = sliced_histogram->total_count() / scan_statistics_object->total_count();
          }
        } else {
          selectivity = 1.0f;
        }

        const auto output_vertical_slices = std::make_shared<VerticalStatisticsSlice<ColumnDataType>>();
        output_vertical_slices->set_statistics_object(sliced_statistics_object);

        output_statistics_slice->vertical_slices[left_column_id] = output_vertical_slices;
      }
    }
  });

  // No matches in this Chunk estimated; prune the ChunkStatistics
  if (selectivity == 0) {
    return nullptr;
  }

  // Entire chunk matches; simply return the input
  if (selectivity == 1) {
    return input_statistics_slice;
  }

  // If predicate has a of 0 < selectivity < 1, scale the other columns' SegmentStatistics that we didn't write to above
  // with the selectivity we determined
  if (output_statistics_slice != input_statistics_slice) {
    for (auto column_id = ColumnID{0}; column_id < input_statistics_slice->vertical_slices.size(); ++column_id) {
      if (!output_statistics_slice->vertical_slices[column_id]) {
        output_statistics_slice->vertical_slices[column_id] =
            input_statistics_slice->vertical_slices[column_id]->scaled(selectivity);
      }
    }

    // Adjust ChunkStatistics row_count
    output_statistics_slice->row_count = input_statistics_slice->row_count * selectivity;
  }

  return output_statistics_slice;
}

}  // namespace opossum
