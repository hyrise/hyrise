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

TEST_F(SortNodeTest, UnchangedColumnMapping) {
  auto sort_node = std::make_shared<SortNode>(
      std::vector<OrderByDefinition>{OrderByDefinition{ColumnID{0}, OrderByMode::Ascending}});
  sort_node->set_left_child(_table_node);

  auto column_ids = sort_node->output_column_ids_to_input_column_ids();

  EXPECT_EQ(column_ids.size(), _table_node->output_column_names().size());

  for (ColumnID column_id{0}; column_id < column_ids.size(); ++column_id) {
    EXPECT_EQ(column_ids[column_id], column_id);
  }
}

TEST_F(SortNodeTest, OutputColumnIDs) {
  auto sort_node = std::make_shared<SortNode>(
      std::vector<OrderByDefinition>{OrderByDefinition{ColumnID{0}, OrderByMode::Ascending}});
  sort_node->set_left_child(_table_node);

  // Valid table name
  auto column_ids = sort_node->get_output_column_ids_for_table("table_a");

  EXPECT_EQ(column_ids.size(), _table_node->output_column_names().size());

  for (ColumnID column_id{0}; column_id < column_ids.size(); ++column_id) {
    EXPECT_EQ(column_ids[column_id], column_id);
  }

  // Invalid table name
  column_ids = sort_node->get_output_column_ids_for_table("invalid_table");
  EXPECT_EQ(column_ids.size(), 0u);
}

}  // namespace opossum
