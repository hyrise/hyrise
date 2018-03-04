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

    _table_node = StoredTableNode::make("table_a");

    _a_a = LQPColumnReference{_table_node, ColumnID{0}};
    _a_b = LQPColumnReference{_table_node, ColumnID{1}};
    _a_c = LQPColumnReference{_table_node, ColumnID{2}};

    _sort_node =
        SortNode::make(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Ascending}}, _table_node);
  }

  std::shared_ptr<StoredTableNode> _table_node;
  std::shared_ptr<SortNode> _sort_node;
  LQPColumnReference _a_a, _a_b, _a_c;
};

TEST_F(SortNodeTest, Descriptions) {
  EXPECT_EQ(_sort_node->description(), "[Sort] table_a.i (Ascending)");

  auto sort_b = SortNode::make(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Descending}});
  sort_b->set_left_input(_table_node);
  EXPECT_EQ(sort_b->description(), "[Sort] table_a.i (Descending)");

  auto sort_c = SortNode::make(std::vector<OrderByDefinition>{OrderByDefinition{_a_c, OrderByMode::Descending},
                                                              OrderByDefinition{_a_b, OrderByMode::Ascending},
                                                              OrderByDefinition{_a_a, OrderByMode::Descending}});
  sort_c->set_left_input(_table_node);
  EXPECT_EQ(sort_c->description(), "[Sort] table_a.d (Descending), table_a.f (Ascending), table_a.i (Descending)");
}

TEST_F(SortNodeTest, UnchangedColumnMapping) {
  auto column_references = _sort_node->output_column_references();

  EXPECT_EQ(column_references.size(), _table_node->output_column_names().size());

  for (ColumnID column_id{0}; column_id < column_references.size(); ++column_id) {
    EXPECT_EQ(column_references[column_id], LQPColumnReference(_table_node, column_id));
  }
}

TEST_F(SortNodeTest, OutputColumnIDs) {
  EXPECT_EQ(_sort_node->find_table_name_origin("table_a"), _table_node);
  EXPECT_EQ(_sort_node->find_table_name_origin("invalid_table"), nullptr);
}

TEST_F(SortNodeTest, ShallowEquals) {
  EXPECT_TRUE(_sort_node->shallow_equals(*_sort_node));

  const auto other_sort_node_a =
      SortNode::make(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Ascending}}, _table_node);
  const auto other_sort_node_b =
      SortNode::make(std::vector<OrderByDefinition>{OrderByDefinition{_a_a, OrderByMode::Ascending},
                                                    OrderByDefinition{_a_b, OrderByMode::Ascending}},
                     _table_node);
  const auto other_sort_node_c =
      SortNode::make(std::vector<OrderByDefinition>{OrderByDefinition{_a_b, OrderByMode::Ascending}}, _table_node);

  EXPECT_TRUE(other_sort_node_a->shallow_equals(*_sort_node));
  EXPECT_FALSE(other_sort_node_b->shallow_equals(*_sort_node));
  EXPECT_FALSE(other_sort_node_c->shallow_equals(*_sort_node));
}

}  // namespace opossum
