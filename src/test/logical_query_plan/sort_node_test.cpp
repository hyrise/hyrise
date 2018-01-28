#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace opossum {

class SortNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_float_double_string.tbl", 2));

    _table_node = std::make_shared<StoredTableNode>("table_a");

    _a_a = LQPColumnReference{_table_node, ColumnID{0}};
    _a_b = LQPColumnReference{_table_node, ColumnID{1}};
    _a_c = LQPColumnReference{_table_node, ColumnID{2}};
  }

  std::shared_ptr<StoredTableNode> _table_node;
  LQPColumnReference _a_a, _a_b, _a_c;
};

TEST_F(SortNodeTest, Descriptions) {
  auto sort_a =
      std::make_shared<SortNode>(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Ascending}});
  sort_a->set_left_child(_table_node);
  EXPECT_EQ(sort_a->description(), "[Sort] table_a.i (Ascending)");

  auto sort_b =
      std::make_shared<SortNode>(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Descending}});
  sort_b->set_left_child(_table_node);
  EXPECT_EQ(sort_b->description(), "[Sort] table_a.i (Descending)");

  auto sort_c = std::make_shared<SortNode>(std::vector<OrderByDefinition>{
      OrderByDefinition{_a_c, OrderByMode::Descending}, OrderByDefinition{_a_b, OrderByMode::Ascending},
      OrderByDefinition{_a_a, OrderByMode::Descending}});
  sort_c->set_left_child(_table_node);
  EXPECT_EQ(sort_c->description(), "[Sort] table_a.d (Descending), table_a.f (Ascending), table_a.i (Descending)");
}

TEST_F(SortNodeTest, UnchangedColumnMapping) {
  auto sort_node =
      std::make_shared<SortNode>(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Ascending}});
  sort_node->set_left_child(_table_node);

  auto column_references = sort_node->output_column_references();

  EXPECT_EQ(column_references.size(), _table_node->output_column_names().size());

  for (ColumnID column_id{0}; column_id < column_references.size(); ++column_id) {
    EXPECT_EQ(column_references[column_id], LQPColumnReference(_table_node, column_id));
  }
}

TEST_F(SortNodeTest, OutputColumnIDs) {
  auto sort_node =
      std::make_shared<SortNode>(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Ascending}});
  sort_node->set_left_child(_table_node);

  EXPECT_EQ(sort_node->find_table_name_origin("table_a"), _table_node);
  EXPECT_EQ(sort_node->find_table_name_origin("invalid_table"), nullptr);
}

}  // namespace opossum
