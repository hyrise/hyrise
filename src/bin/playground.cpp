#include <iostream>

#include "operators/join_nested_loop.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/load_table.hpp"
#include "utils/timer.hpp"

using namespace opossum;  // NOLINT

int main() {
  const auto table_left =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int}, {"b", DataType::Int}}, TableType::Data);
  table_left->append({1, 2});
  table_left->append({3, 4});

  const auto table_right =
      std::make_shared<Table>(TableColumnDefinitions{{"x", DataType::Int}, {"y", DataType::Int}}, TableType::Data);
  table_right->append({3, 2});
  table_right->append({5, 6});

  const auto table_left_wrapper = std::make_shared<TableWrapper>(table_left);
  table_left_wrapper->execute();
  const auto table_right_wrapper = std::make_shared<TableWrapper>(table_right);
  table_right_wrapper->execute();

  const auto join_op =
      std::make_shared<JoinSortMerge>(table_left_wrapper, table_right_wrapper, JoinMode::Right,
                                      ColumnIDPair{ColumnID{0}, ColumnID{0}}, PredicateCondition::LessThanEquals);

  join_op->execute();

  Print::print(table_left_wrapper->get_output());
  std::cout << std::endl;

  Print::print(table_right_wrapper->get_output());
  std::cout << std::endl;

  std::cout << join_op->description(DescriptionMode::MultiLine) << std::endl;
  std::cout << std::endl;

  Print::print(join_op->get_output());

  return 0;
}
