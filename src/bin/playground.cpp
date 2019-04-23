#include <iostream>

#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto left = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, true}}, TableType::Data);
  left->append({NullValue{}});

  const auto right = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, true}}, TableType::Data);
  //right->append({5});

  const auto left_op = std::make_shared<TableWrapper>(left);
  const auto right_op = std::make_shared<TableWrapper>(right);
  left_op->execute();
  right_op->execute();

  const auto join_op =
      std::make_shared<JoinHash>(left_op, right_op, JoinMode::AntiNullAsTrue,
                                 OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});

  join_op->execute();

  Print::print(join_op->get_output());

  return 0;
}
