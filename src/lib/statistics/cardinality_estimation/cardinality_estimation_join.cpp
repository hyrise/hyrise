#include "cardinality_estimation_join.hpp"

#include <iostream>

#include "statistics/chunk_statistics2.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"
#include "operators/operator_join_predicate.hpp"
#include "resolve_type.hpp"

namespace opossum {

std::shared_ptr<TableStatistics2> cardinality_estimation_predicated_join(
    const JoinMode join_mode, const OperatorJoinPredicate& join_predicate,
    const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics2>& right_input_table_statistics) {

  const auto output_table_statistics = std::make_shared<TableStatistics2>();
  output_table_statistics->chunk_statistics.reserve(left_input_table_statistics->chunk_statistics.size() *
                                                    right_input_table_statistics->chunk_statistics.size());

  switch (join_mode) {
    case JoinMode::Semi:
    case JoinMode::Anti: {
      // TODO(anybody) Handle properly
      return left_input_table_statistics;
    }

    // TODO(anybody) For now, handle outer joins just as inner joins
    case JoinMode::Left:
    case JoinMode::Right:
    case JoinMode::Outer:
    case JoinMode::Inner:
        if (join_predicate.predicate_condition == PredicateCondition::Equals) {
          if (left_input_table_statistics->chunk_statistics.empty() || right_input_table_statistics->chunk_statistics.empty()) {
            return output_table_statistics;
          }

          const auto left_column_id = join_predicate.column_ids.first;
          const auto right_column_id = join_predicate.column_ids.second;
          const auto left_data_type = left_input_table_statistics->chunk_statistics.front()->segment_statistics[left_column_id]->data_type;
          const auto right_data_type = right_input_table_statistics->chunk_statistics.front()->segment_statistics[right_column_id]->data_type;

          // TODO(anybody)
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
                  left_input_segment_statistics->get_best_available_histogram();
                  const auto right_histogram =
                  right_input_segment_statistics->get_best_available_histogram();

                  // TODO(anybody)
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
        } else {
          // TODO(anybody)
          return cardinality_estimation_cross_join(left_input_table_statistics, right_input_table_statistics);
        }
      }
    } break;

    case JoinMode::Cross:
      Fail("Cross join is not a predicated join");
  }
}

std::shared_ptr<TableStatistics2> cardinality_estimation_cross_join(
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

}  // namespace opossum