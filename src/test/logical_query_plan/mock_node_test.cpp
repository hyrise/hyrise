#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/table_statistics.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class MockNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto table = load_table("resources/test_data/tbl/int_float_double_string.tbl");
    _statistics = std::make_shared<TableStatistics>(generate_table_statistics(*table));

    _mock_node_a = MockNode::make(MockNode::ColumnDefinitions{
        {DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::Double, "c"}, {DataType::String, "d"}});
    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<TableStatistics> _statistics;
};

TEST_F(MockNodeTest, Description) {
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed'] pruned: 0/4 columns");
  EXPECT_EQ(_mock_node_b->description(), "[MockNode 'mock_name'] pruned: 0/2 columns");

  _mock_node_a->set_pruned_column_ids({ColumnID{2}});
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed'] pruned: 1/4 columns");
}

TEST_F(MockNodeTest, OutputColumnExpression) {
  ASSERT_EQ(_mock_node_a->column_expressions().size(), 4u);
  EXPECT_EQ(*_mock_node_a->column_expressions().at(0), *lqp_column_({_mock_node_a, ColumnID{0}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(1), *lqp_column_({_mock_node_a, ColumnID{1}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(2), *lqp_column_({_mock_node_a, ColumnID{2}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(3), *lqp_column_({_mock_node_a, ColumnID{3}}));

  ASSERT_EQ(_mock_node_b->column_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_b->column_expressions().at(0), *lqp_column_({_mock_node_b, ColumnID{0}}));
  EXPECT_EQ(*_mock_node_b->column_expressions().at(1), *lqp_column_({_mock_node_b, ColumnID{1}}));

  _mock_node_a->set_pruned_column_ids({ColumnID{0}, ColumnID{3}});
  EXPECT_EQ(_mock_node_a->column_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_a->column_expressions().at(0), *lqp_column_({_mock_node_a, ColumnID{1}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(1), *lqp_column_({_mock_node_a, ColumnID{2}}));
}

TEST_F(MockNodeTest, Equals) {
  //
  const auto same_mock_node_b =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  EXPECT_EQ(*_mock_node_b, *_mock_node_b);
  EXPECT_EQ(*_mock_node_b, *same_mock_node_b);
}

TEST_F(MockNodeTest, Copy) {
  EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy());

  _mock_node_b->set_pruned_column_ids({ColumnID{1}});
  EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy());
}

TEST_F(MockNodeTest, NodeExpressions) { ASSERT_EQ(_mock_node_a->node_expressions.size(), 0u); }

TEST_F(MockNodeTest, GetStatistics) {
  _mock_node_a->set_statistics(_statistics);

  EXPECT_EQ(_mock_node_a->get_statistics()->column_statistics().size(), 4u);

  const auto column_statistics_b = _mock_node_a->get_statistics()->column_statistics().at(1u);

  _mock_node_a->set_pruned_column_ids({ColumnID{0}});
  EXPECT_EQ(_mock_node_a->get_statistics()->column_statistics().size(), 3u);
  EXPECT_EQ(_mock_node_a->get_statistics()->column_statistics().at(0u), column_statistics_b);
}

}  // namespace opossum
