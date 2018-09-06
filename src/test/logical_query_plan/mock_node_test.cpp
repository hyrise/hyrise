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

    _mock_node_a = MockNode::make(MockNode::CxlumnDefinitions{
        {DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::Double, "c"}, {DataType::String, "d"}});
    _mock_node_b =
        MockNode::make(MockNode::CxlumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<TableStatistics> _statistics;
};

TEST_F(MockNodeTest, Description) {
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed']");
  EXPECT_EQ(_mock_node_b->description(), "[MockNode 'mock_name']");
}

TEST_F(MockNodeTest, OutputCxlumnExpression) {
  ASSERT_EQ(_mock_node_a->cxlumn_expressions().size(), 4u);
  EXPECT_EQ(*_mock_node_a->cxlumn_expressions().at(0), *cxlumn_({_mock_node_a, CxlumnID{0}}));
  EXPECT_EQ(*_mock_node_a->cxlumn_expressions().at(1), *cxlumn_({_mock_node_a, CxlumnID{1}}));
  EXPECT_EQ(*_mock_node_a->cxlumn_expressions().at(2), *cxlumn_({_mock_node_a, CxlumnID{2}}));
  EXPECT_EQ(*_mock_node_a->cxlumn_expressions().at(3), *cxlumn_({_mock_node_a, CxlumnID{3}}));

  ASSERT_EQ(_mock_node_b->cxlumn_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_b->cxlumn_expressions().at(0), *cxlumn_({_mock_node_b, CxlumnID{0}}));
  EXPECT_EQ(*_mock_node_b->cxlumn_expressions().at(1), *cxlumn_({_mock_node_b, CxlumnID{1}}));
}

TEST_F(MockNodeTest, Equals) {
  //
  const auto same_mock_node_b =
      MockNode::make(MockNode::CxlumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  EXPECT_EQ(*_mock_node_b, *_mock_node_b);
  EXPECT_EQ(*_mock_node_b, *same_mock_node_b);
}

TEST_F(MockNodeTest, Copy) { EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy()); }

}  // namespace opossum
