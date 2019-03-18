#include <iostream>

#include "types.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "operators/join_hash.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto table_a = load_table("resources/test_data/tbl/join_operators/multi_predicates/int_int_string_nulls_random_a.tbl", 4);
  const auto table_b = load_table("resources/test_data/tbl/join_operators/multi_predicates/string_int_int_nulls_random_b_larger.tbl", 4);

  const auto table_a_op = std::make_shared<TableWrapper>(table_a);
  const auto table_b_op = std::make_shared<TableWrapper>(table_b);
  table_a_op->execute();
  table_b_op->execute();

  const auto join_op = std::make_shared<JoinHash>(table_a_op, table_b_op, JoinMode::AntiRetainNulls, OperatorJoinPredicate{{ColumnID{0}, ColumnID{1}}, PredicateCondition::Equals}, 2);

  join_op->execute();

  Print::print(join_op->get_output());

  return 0;
}
