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
    auto table = load_table("src/test/tables/int_float_double_string.tbl", Chunk::MAX_SIZE);
    _statistics = std::make_shared<TableStatistics>(generate_table_statistics(*table));

    _mock_node_a = MockNode::make(_statistics);
    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<TableStatistics> _statistics;
};

TEST_F(MockNodeTest, Description) {
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed']");
  EXPECT_EQ(_mock_node_b->description(), "[MockNode 'mock_name']");
}

TEST_F(MockNodeTest, OutputColumnExpression) {
  ASSERT_EQ(_mock_node_a->column_expressions().size(), 4u);
  EXPECT_EQ(*_mock_node_a->column_expressions().at(0), *column_({_mock_node_a, ColumnID{0}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(1), *column_({_mock_node_a, ColumnID{1}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(2), *column_({_mock_node_a, ColumnID{2}}));
  EXPECT_EQ(*_mock_node_a->column_expressions().at(3), *column_({_mock_node_a, ColumnID{3}}));

  ASSERT_EQ(_mock_node_b->column_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_b->column_expressions().at(0), *column_({_mock_node_b, ColumnID{0}}));
  EXPECT_EQ(*_mock_node_b->column_expressions().at(1), *column_({_mock_node_b, ColumnID{1}}));
}

TEST_F(MockNodeTest, Equals) {
  // Consume the result of an equality check to avoid "equality comparison result unused [-Werror,-Wunused-comparison]"
  // errors
  const auto dummy = [](const auto v) {};

  // Can't compare MockNodes with statistics
  EXPECT_ANY_THROW(dummy(*_mock_node_a == *_mock_node_a));
  const auto other_mock_node_a = MockNode::make(_statistics);
  EXPECT_ANY_THROW(dummy(*_mock_node_a == *other_mock_node_a));

  //
  const auto same_mock_node_b =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  EXPECT_EQ(*_mock_node_b, *_mock_node_b);
  EXPECT_EQ(*_mock_node_b, *same_mock_node_b);
}

TEST_F(MockNodeTest, Copy) { EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy()); }

}  // namespace opossum
