#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class operators_sort : public ::testing::Test {
  virtual void SetUp() {
    _test_table = std::make_shared<opossum::Table>(opossum::Table(2));

    _test_table->add_column("a", "int");
    _test_table->add_column("b", "float");

    _test_table->append({12345, 458.7f});
    _test_table->append({123, 456.7f});
    _test_table->append({1234, 457.7f});

    opossum::StorageManager::get().add_table("table_a", std::move(_test_table));

    _gt = std::make_shared<opossum::GetTable>("table_a");
  }

 public:
  std::shared_ptr<opossum::Table> _test_table;
  std::shared_ptr<opossum::GetTable> _gt;
};

TEST_F(operators_sort, test_ascending_sort_of_one_column) {
  auto sort = std::make_shared<Sort>(_gt, "a");
  sort->execute();

  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[0]), 123);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[1]), 1234);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[2]), 12345);
}

TEST_F(operators_sort, test_double_sort_of_one_column) {
  auto sort1 = std::make_shared<Sort>(_gt, "a", false);
  sort1->execute();

  auto sort2 = std::make_shared<Sort>(_gt, "a");
  sort2->execute();

  EXPECT_EQ(type_cast<int>((*(sort2->get_output()->get_chunk(0).get_column(0)))[0]), 123);
  EXPECT_EQ(type_cast<int>((*(sort2->get_output()->get_chunk(0).get_column(0)))[1]), 1234);
  EXPECT_EQ(type_cast<int>((*(sort2->get_output()->get_chunk(0).get_column(0)))[2]), 12345);
}

TEST_F(operators_sort, test_descending_sort_of_one_column) {
  auto sort = std::make_shared<Sort>(_gt, "a", false);
  sort->execute();

  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[0]), 12345);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[1]), 1234);
  EXPECT_EQ(type_cast<int>((*(sort->get_output()->get_chunk(0).get_column(0)))[2]), 123);
}
}