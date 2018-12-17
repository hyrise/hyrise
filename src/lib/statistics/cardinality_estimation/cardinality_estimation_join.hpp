#pragma once

#include "types.hpp"

namespace opossum {

class OperatorJoinPredicate;
class TableStatistics2;

std::shared_ptr<TableStatistics2> cardinality_estimation_predicated_join(
    const JoinMode join_mode,const OperatorJoinPredicate& join_predicate,
    const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
    const std::shared_ptr<TableStatistics2>& right_input_table_statistics);


std::shared_ptr<TableStatistics2> cardinality_estimation_cross_join(
const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
const std::shared_ptr<TableStatistics2>& right_input_table_statistics);

}  // namespace opossum
