#include <iostream>

#include "types.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/print.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto left = load_table("resources/test_data/tbl/join_operators/generated_tables/join_table_left_10.tbl");
  const auto right = load_table("resources/test_data/tbl/join_operators/generated_tables/join_table_right_10.tbl");

  const auto left_op = std::make_shared<TableWrapper>(left);
  const auto right_op = std::make_shared<TableWrapper>(right);
  left_op->execute();
  right_op->execute();

  const auto join_op = std::make_shared<JoinSortMerge>(left_op, right_op, JoinMode::Left, OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::GreaterThan});

  join_op->execute();

  Print::print(join_op->get_output());

  return 0;
}
