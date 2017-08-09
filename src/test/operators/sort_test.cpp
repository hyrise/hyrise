#include <iostream>
#include <memory>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsSortTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));

    _table_wrapper->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper;
};

TEST_F(OperatorsSortTest, AscendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, true, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, AscendingSortOfOneColumnWithoutChunkSize) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, true);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DoubleSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_sorted.tbl", 2);

  auto sort1 = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, false, 2u);
  sort1->execute();

  auto sort2 = std::make_shared<Sort>(sort1, ColumnID{0}, true, 2u);
  sort2->execute();

  EXPECT_TABLE_EQ(sort2->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<Sort>(_table_wrapper, ColumnID{0}, false, 2u);
  sort->execute();

  EXPECT_TABLE_EQ(sort->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, MultipleColumnSort) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float2.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float2_sorted.tbl", 2);

  // we want the output to be sorted after column a and in second place after column b.
  // So first we sort after column b and then after column a.

  auto sort_after_b = std::make_shared<Sort>(table_wrapper, ColumnID{1}, true, 2u);
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<Sort>(sort_after_b, ColumnID{0}, true, 2u);
  sort_after_a->execute();

  EXPECT_TABLE_EQ(sort_after_a->get_output(), expected_result, true);
}

TEST_F(OperatorsSortTest, MultipleColumnSortIsStable) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float2_sorted.tbl", 2);

  // we want the output to be sorted after column a and in second place after column b.
  // So first we sort after column b and then after column a.

  auto sort_after_b = std::make_shared<Sort>(table_wrapper, ColumnID{1}, true, 2u);
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<Sort>(sort_after_b, ColumnID{0}, true, 2u);
  sort_after_a->execute();

  EXPECT_TABLE_EQ(sort_after_a->get_output(), expected_result, true);
}

}  // namespace opossum
