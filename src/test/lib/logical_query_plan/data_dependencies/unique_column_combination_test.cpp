#include <memory>
#include <vector>

#include "base_test.hpp"
#include "logical_query_plan/data_dependencies/unique_column_combination.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "types.hpp"

namespace hyrise {

class UniqueColumnCombinationTest : public BaseTest {
 public:
  void SetUp() override {
    _mock_node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "mock_node_a");
    _a = _mock_node_a->get_column("a");
    _b = _mock_node_a->get_column("b");
    _c = _mock_node_a->get_column("c");

    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}}, "mock_node_b");
    _x = _mock_node_b->get_column("x");
    _y = _mock_node_b->get_column("y");
  }

 protected:
  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c, _x, _y;
};

TEST_F(UniqueColumnCombinationTest, Equals) {
  const auto ucc_a = UniqueColumnCombination{{_a}};
  const auto ucc_a_b_c = UniqueColumnCombination{{_a, _b, _c}};

  // Equal.
  EXPECT_EQ(ucc_a, UniqueColumnCombination({_a}));
  EXPECT_EQ(ucc_a_b_c, UniqueColumnCombination({_a, _b, _c}));
  EXPECT_EQ(ucc_a_b_c, UniqueColumnCombination({_b, _a, _c}));
  EXPECT_EQ(ucc_a_b_c, UniqueColumnCombination({_b, _c, _a}));

  // Not Equal.
  EXPECT_NE(ucc_a, UniqueColumnCombination({_a, _b}));
  EXPECT_NE(ucc_a, UniqueColumnCombination({_b}));
  EXPECT_NE(ucc_a_b_c, UniqueColumnCombination({_a, _b}));
  EXPECT_NE(ucc_a_b_c, UniqueColumnCombination({_a, _b, _c, _x}));
}

TEST_F(UniqueColumnCombinationTest, Hash) {
  const auto ucc_a = UniqueColumnCombination{{_a}};
  const auto ucc_a_b_c = UniqueColumnCombination{{_a, _b, _c}};

  EXPECT_EQ(ucc_a.hash(), UniqueColumnCombination({_a}).hash());
  EXPECT_EQ(ucc_a_b_c.hash(), UniqueColumnCombination({_a, _b, _c}).hash());
  EXPECT_EQ(ucc_a_b_c.hash(), UniqueColumnCombination({_c, _a, _b}).hash());
  EXPECT_EQ(ucc_a_b_c.hash(), UniqueColumnCombination({_c, _b, _a}).hash());
}

TEST_F(UniqueColumnCombinationTest, ToStream) {
  auto stream = std::stringstream{};

  stream << UniqueColumnCombination{{_a}};
  EXPECT_EQ(stream.str(), "{a}");
  stream.str("");

  stream << UniqueColumnCombination{{_a, _b, _c}};
  EXPECT_EQ(stream.str(), "{a, b, c}");
  stream.str("");

  stream << UniqueColumnCombination{{_b, _c, _a}};
  EXPECT_EQ(stream.str(), "{a, b, c}");
  stream.str("");

  stream << UniqueColumnCombination{{_c, _b, _a}};
  EXPECT_EQ(stream.str(), "{a, b, c}");
}

}  // namespace hyrise
