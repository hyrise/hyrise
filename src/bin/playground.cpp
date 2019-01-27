#include <iostream>

#include "operators/join_sort_merge.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto table_left = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}}, TableType::Data);
  table_left->append({95});
  const auto table_right = std::make_shared<Table>(TableColumnDefinitions{{"x", DataType::Int}}, TableType::Data);
  table_right->append({94});

  const auto table_left_wrapper = std::make_shared<TableWrapper>(table_left);
  table_left_wrapper->execute();

  const auto table_right_wrapper = std::make_shared<TableWrapper>(table_right);
  table_right_wrapper->execute();

  const auto join = std::make_shared<JoinSortMerge>(table_left_wrapper, table_right_wrapper, JoinMode::Left,
                                                    ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals);
  join->execute();

  Print::print(join->get_output());

  return 0;
}
