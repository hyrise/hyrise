#pragma once

#include "types.hpp"

namespace opossum {

struct OperatorJoinPredicate;
class TableStatistics2;
template <typename T> class AbstractHistogram;
template <typename T> class GenericHistogram;

template <typename T>
std::shared_ptr<GenericHistogram<T>> estimate_histogram_of_inner_equi_join_with_bin_adjusted_histograms(
const std::shared_ptr<AbstractHistogram<T>>& histogram_left,
const std::shared_ptr<AbstractHistogram<T>>& histogram_right);

std::shared_ptr<TableStatistics2> cardinality_estimation_inner_equi_join(const ColumnID left_column_id, const ColumnID right_column_id,
                                                                         const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
                                                                         const std::shared_ptr<TableStatistics2>& right_input_table_statistics);

std::shared_ptr<TableStatistics2> cardinality_estimation_inner_join(const OperatorJoinPredicate& join_predicate,
                                                                    const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
                                                                    const std::shared_ptr<TableStatistics2>& right_input_table_statistics);

std::shared_ptr<TableStatistics2> cardinality_estimation_predicated_join(
    const JoinMode join_mode,const OperatorJoinPredicate& join_predicate,
    const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics2>& right_input_table_statistics);


std::shared_ptr<TableStatistics2> cardinality_estimation_cross_join(
const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
const std::shared_ptr<TableStatistics2>& right_input_table_statistics);

}  // namespace opossum
