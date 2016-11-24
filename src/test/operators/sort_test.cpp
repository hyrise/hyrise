#include <iostream>
#include <memory>
#include <utility>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsSortTest : public BaseTest {
  void SetUp() override {
    _test_table = std::make_shared<Table>(Table(2));

    _test_table->add_column("a", "int");
    _test_table->add_column("b", "float");

    _test_table->append({12345, 458.7f});
    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});

    StorageManager::get().add_table("table_a", std::move(_test_table));

    _gt = std::make_shared<GetTable>("table_a");
  }

 public:
  std::shared_ptr<Table> _test_table;
  std::shared_ptr<GetTable> _gt;
};

TEST_F(OperatorsSortTest, AscendingSortOfOneColumn) {
  auto sort = std::make_shared<Sort>(_gt, "a");
  sort->execute();

  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[0]), 123);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[1]), 1234);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[2]), 12345);
}

TEST_F(OperatorsSortTest, DoubleSortOfOneColumn) {
  auto sort1 = std::make_shared<Sort>(_gt, "a", false);
  sort1->execute();

  auto sort2 = std::make_shared<Sort>(_gt, "a");
  sort2->execute();

  EXPECT_EQ(type_cast<int>((*(sort2->get_output()->get_chunk(0).get_column(0)))[0]), 123);
  EXPECT_EQ(type_cast<int>((*(sort2->get_output()->get_chunk(0).get_column(0)))[1]), 1234);
  EXPECT_EQ(type_cast<int>((*(sort2->get_output()->get_chunk(0).get_column(0)))[2]), 12345);
}

TEST_F(OperatorsSortTest, DescendingSortOfOneColumn) {
  auto sort = std::make_shared<Sort>(_gt, "a", false);
  sort->execute();

  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[0]), 12345);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[1]), 1234);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[2]), 123);
}

TEST_F(OperatorsSortTest, multiple_column_sort) {
  auto test_table_b = std::make_shared<Table>(Table(2));

  test_table_b->add_column("a", "int");
  test_table_b->add_column("b", "float");

  test_table_b->append({12345, 456.7f});
  test_table_b->append({12345, 457.7f});
  test_table_b->append({123, 458.7f});

  StorageManager::get().add_table("test_table_sort_b", std::move(test_table_b));

  auto gt = std::make_shared<GetTable>("test_table_sort_b");

  // we want the output to be sorted after column a and in second place after column b. So first we sort after column b
  // and then after column a.

  auto sort_after_b = std::make_shared<Sort>(gt, "b");
  sort_after_b->execute();

  auto sort_after_a = std::make_shared<Sort>(sort_after_b, "a");
  sort_after_a->execute();

  EXPECT_EQ(type_cast<float>((*(sort_after_a->get_output()->get_chunk(0).get_column(1)))[0]), 458.7f);
  EXPECT_EQ(type_cast<float>((*(sort_after_a->get_output()->get_chunk(0).get_column(1)))[1]), 456.7f);
  EXPECT_EQ(type_cast<float>((*(sort_after_a->get_output()->get_chunk(0).get_column(1)))[2]), 457.7f);
}
}  // namespace opossum
