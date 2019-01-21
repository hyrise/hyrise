#include "cardinality_estimation_scan.hpp"

#include "operators/operator_scan_predicate.hpp"
#include "resolve_type.hpp"
#include "statistics/abstract_statistics_object.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/chunk_statistics2.hpp"
#include "statistics/segment_statistics2.hpp"

namespace opossum {

template <typename T>
std::shared_ptr<GenericHistogram<T>> estimate_histogram_of_column_to_column_equi_scan_with_bin_adjusted_histograms(
    const std::shared_ptr<AbstractHistogram<T>>& left_histogram,
    const std::shared_ptr<AbstractHistogram<T>>& right_histogram) {
  auto left_idx = BinID{0};
  auto right_idx = BinID{0};
  auto left_bin_count = left_histogram->bin_count();
  auto right_bin_count = right_histogram->bin_count();

  std::vector<T> bin_minima;
  std::vector<T> bin_maxima;
  std::vector<HistogramCountType> bin_heights;
  std::vector<HistogramCountType> bin_distinct_counts;

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

    const auto left_distinct_count = left_histogram->bin_distinct_count(left_idx);
    const auto right_distinct_count = right_histogram->bin_distinct_count(right_idx);

    const auto min_distinct_count = std::min(left_distinct_count, right_distinct_count);

    if (min_distinct_count > 0.0f) {
      const auto eyssen_zimmermannsche_unschaerfe = std::min(
          (static_cast<float>(min_distinct_count) / left_distinct_count) * left_histogram->bin_height(left_idx),
          (static_cast<float>(min_distinct_count) / right_distinct_count) * right_histogram->bin_height(right_idx));

      if (eyssen_zimmermannsche_unschaerfe > 0.0f) {
        bin_minima.emplace_back(left_min);
        bin_maxima.emplace_back(left_histogram->bin_maximum(left_idx));
        bin_heights.emplace_back(std::ceil(eyssen_zimmermannsche_unschaerfe));
        bin_distinct_counts.emplace_back(min_distinct_count);
      }
    }

    ++left_idx;
    ++right_idx;
  }

  if (bin_minima.empty()) {
    return nullptr;
  }

  return std::make_shared<GenericHistogram<T>>(std::move(bin_minima), std::move(bin_maxima), std::move(bin_heights),
                                               std::move(bin_distinct_counts));
}

std::shared_ptr<ChunkStatistics2> cardinality_estimation_chunk_scan(
    const std::shared_ptr<ChunkStatistics2>& input_chunk_statistics, const OperatorScanPredicate& predicate) {
  // TODO(anybody) use IsNullStatisticsObject
  if (predicate.predicate_condition == PredicateCondition::IsNull) {
    return nullptr;
  } else if (predicate.predicate_condition == PredicateCondition::IsNotNull) {
    return input_chunk_statistics;
  }

  auto output_chunk_statistics = std::make_shared<ChunkStatistics2>();
  output_chunk_statistics->segment_statistics.resize(input_chunk_statistics->segment_statistics.size());

  auto selectivity = Selectivity{1};

  const auto left_column_id = predicate.column_id;
  auto right_column_id = std::optional<ColumnID>{};

  /**
   * Manipulate statistics of column that we scan on
   */
  const auto base_segment_statistics = input_chunk_statistics->segment_statistics.at(left_column_id);

  const auto left_data_type = input_chunk_statistics->segment_statistics[left_column_id]->data_type;

  resolve_data_type(left_data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;

    const auto segment_statistics =
        std::static_pointer_cast<SegmentStatistics2<ColumnDataType>>(base_segment_statistics);

    auto primary_scan_statistics_object = segment_statistics->get_best_available_histogram();
    // If there are no statistics available for this segment, assume a selectivity of 1
    if (!primary_scan_statistics_object) {
      output_chunk_statistics = input_chunk_statistics;
      return;
    }

    if (predicate.value.type() == typeid(ColumnID)) {
      right_column_id = boost::get<ColumnID>(predicate.value);

      const auto right_data_type = input_chunk_statistics->segment_statistics[*right_column_id]->data_type;

      if (left_data_type != right_data_type) {
        // TODO(anybody)
        output_chunk_statistics = input_chunk_statistics;
        return;
      }

      if (predicate.predicate_condition != PredicateCondition::Equals) {
        // TODO(anyone) CardinalityEstimator cannot handle non-equi column-to-column scans right now
        output_chunk_statistics = input_chunk_statistics;
        return;
      }

      const auto left_input_segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
          input_chunk_statistics->segment_statistics[left_column_id]);
      const auto right_input_segment_statistics = std::dynamic_pointer_cast<SegmentStatistics2<ColumnDataType>>(
          input_chunk_statistics->segment_statistics[*right_column_id]);

      const auto left_histogram = left_input_segment_statistics->get_best_available_histogram();
      const auto right_histogram = right_input_segment_statistics->get_best_available_histogram();
      Assert(left_histogram && right_histogram, "NYI");

      const auto bin_adjusted_left_histogram = left_histogram->split_at_bin_bounds(right_histogram->bin_bounds());
      const auto bin_adjusted_right_histogram = right_histogram->split_at_bin_bounds(left_histogram->bin_bounds());

      const auto column_to_column_histogram =
          estimate_histogram_of_column_to_column_equi_scan_with_bin_adjusted_histograms(bin_adjusted_left_histogram,
                                                                                        bin_adjusted_right_histogram);
      if (!column_to_column_histogram) {
        // No matches in this Chunk estimated; prune the ChunkStatistics
        output_chunk_statistics = nullptr;
        return;
      }

      const auto cardinality = column_to_column_histogram->total_count();
      selectivity = cardinality / input_chunk_statistics->row_count;

      /**
       * Write out the SegmentStatistics
       */
      const auto output_segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
      output_segment_statistics->set_statistics_object(column_to_column_histogram);
      output_chunk_statistics->segment_statistics[left_column_id] = output_segment_statistics;
      output_chunk_statistics->segment_statistics[*right_column_id] = output_segment_statistics;

    } else if (predicate.value.type() == typeid(ParameterID)) {
      // For predicates involving placeholders, assume a selectivity of 1
      output_chunk_statistics = input_chunk_statistics;

    } else {
      Assert(predicate.value.type() == typeid(AllTypeVariant), "Expected AllTypeVariant");

      auto value2_all_type_variant = std::optional<AllTypeVariant>{};
      if (predicate.value2) {
        Assert(predicate.value2->type() == typeid(AllTypeVariant),
               "Histogram can't handle column-to-column scans right now");
        value2_all_type_variant = boost::get<AllTypeVariant>(*predicate.value2);
      }

      const auto sliced_statistics_object = primary_scan_statistics_object->sliced_with_predicate(
          predicate.predicate_condition, boost::get<AllTypeVariant>(predicate.value), value2_all_type_variant);

      const auto cardinality_estimate = primary_scan_statistics_object->estimate_cardinality(
          predicate.predicate_condition, boost::get<AllTypeVariant>(predicate.value), value2_all_type_variant);

      if (input_chunk_statistics->row_count == 0 || cardinality_estimate.type == EstimateType::MatchesNone) {
        // No matches in this Chunk estimated; prune the ChunkStatistics
        output_chunk_statistics = nullptr;
        return;
      } else {
        selectivity = cardinality_estimate.cardinality / primary_scan_statistics_object->total_count();
      }

      const auto output_segment_statistics = std::make_shared<SegmentStatistics2<ColumnDataType>>();
      output_segment_statistics->set_statistics_object(sliced_statistics_object);

      output_chunk_statistics->segment_statistics[left_column_id] = output_segment_statistics;
    }
  });

  // No matches in this Chunk estimated; prune the ChunkStatistics
  if (!output_chunk_statistics) return nullptr;

  // If predicate has a selectivity of != 1, scale the other columns' SegmentStatistics
  if (output_chunk_statistics != input_chunk_statistics) {
    /**
     * Manipulate statistics of all columns that we DIDN'T scan on with this predicate
     */
    for (auto column_id = ColumnID{0}; column_id < input_chunk_statistics->segment_statistics.size(); ++column_id) {
      if (column_id == left_column_id || (right_column_id && column_id == *right_column_id)) continue;

      output_chunk_statistics->segment_statistics[column_id] =
          input_chunk_statistics->segment_statistics[column_id]->scaled_with_selectivity(selectivity);
    }

    /**
     * Adjust ChunkStatistics row_count
     */
    output_chunk_statistics->row_count = input_chunk_statistics->row_count * selectivity;
  }

  return output_chunk_statistics;
}

}  // namespace opossum
