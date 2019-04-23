#include <iostream>

#include "operators/join_sort_merge.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto left = load_table("resources/test_data/tbl/join_test_runner/input_table_left_10_x.tbl");
  const auto right = load_table("resources/test_data/tbl/join_test_runner/input_table_right_10_x.tbl");

  const auto left_op = std::make_shared<TableWrapper>(left);
  const auto right_op = std::make_shared<TableWrapper>(right);
  left_op->execute();
  right_op->execute();

  const auto join_op = std::make_shared<JoinSortMerge>(
      left_op, right_op, JoinMode::Right,
      OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}, std::vector<OperatorJoinPredicate>{
        OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThanEquals}
      });

  join_op->execute();

  Print::print(join_op->get_output());

  return 0;
}
