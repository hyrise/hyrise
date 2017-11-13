#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/sort_node.hpp"
#include "optimizer/abstract_syntax_tree/stored_table_node.hpp"

namespace opossum {

class SortNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float_double_string.tbl", 2));

    _table_node = std::make_shared<StoredTableNode>("table_a");
  }

  std::shared_ptr<StoredTableNode> _table_node;
};

TEST_F(SortNodeTest, Descriptions) {
  auto sort_a = std::make_shared<SortNode>(
      std::vector<OrderByDefinition>{OrderByDefinition{ColumnID{0}, OrderByMode::Ascending}});
  sort_a->set_left_child(_table_node);
  EXPECT_EQ(sort_a->description(), "[Sort] table_a.i (Ascending)");

  auto sort_b = std::make_shared<SortNode>(
      std::vector<OrderByDefinition>{OrderByDefinition{ColumnID{0}, OrderByMode::Descending}});
  sort_b->set_left_child(_table_node);
  EXPECT_EQ(sort_b->description(), "[Sort] table_a.i (Descending)");

  auto sort_c = std::make_shared<SortNode>(std::vector<OrderByDefinition>{
      OrderByDefinition{ColumnID{2}, OrderByMode::Descending}, OrderByDefinition{ColumnID{1}, OrderByMode::Ascending},
      OrderByDefinition{ColumnID{0}, OrderByMode::Descending}});
  sort_c->set_left_child(_table_node);
  EXPECT_EQ(sort_c->description(), "[Sort] table_a.d (Descending), table_a.f (Ascending), table_a.i (Descending)");
}

}  // namespace opossum
