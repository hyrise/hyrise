#include "base_test.hpp"

#include "logical_query_plan/functional_dependency.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class LQPUniqueConstraintTest : public BaseTest {
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
  std::shared_ptr<MockNode> _mock_node_a, _mock_node_b;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c, _x, _y;
};

TEST_F(LQPUniqueConstraintTest, Equals) {
  const auto unique_constraint_a = LQPUniqueConstraint({_a});
  const auto unique_constraint_a_b_c = LQPUniqueConstraint({_a, _b, _c});

  // Equal
  EXPECT_EQ(unique_constraint_a, LQPUniqueConstraint({_a}));
  EXPECT_EQ(unique_constraint_a_b_c, LQPUniqueConstraint({_a, _b, _c}));
  EXPECT_EQ(unique_constraint_a_b_c, LQPUniqueConstraint({_b, _a, _c}));
  EXPECT_EQ(unique_constraint_a_b_c, LQPUniqueConstraint({_b, _c, _a}));
  // Not Equal
  EXPECT_NE(unique_constraint_a, LQPUniqueConstraint({_a, _b}));
  EXPECT_NE(unique_constraint_a, LQPUniqueConstraint({_b}));
  EXPECT_NE(unique_constraint_a_b_c, LQPUniqueConstraint({_a, _b}));
  EXPECT_NE(unique_constraint_a_b_c, LQPUniqueConstraint({_a, _b, _c, _x}));
}

TEST_F(LQPUniqueConstraintTest, Hash) {
  const auto unique_constraint_a = LQPUniqueConstraint({_a});
  const auto unique_constraint_a_b_c = LQPUniqueConstraint({_a, _b, _c});

  // Equal Hash
  EXPECT_EQ(unique_constraint_a.hash(), LQPUniqueConstraint({_a}).hash());
  EXPECT_EQ(unique_constraint_a_b_c.hash(), LQPUniqueConstraint({_a, _b, _c}).hash());
  EXPECT_EQ(unique_constraint_a_b_c.hash(), LQPUniqueConstraint({_c, _a, _b}).hash());
  EXPECT_EQ(unique_constraint_a_b_c.hash(), LQPUniqueConstraint({_c, _b, _a}).hash());

  // Non-Equal Hash
  EXPECT_NE(unique_constraint_a.hash(), LQPUniqueConstraint({_a, _b}).hash());
  EXPECT_NE(unique_constraint_a.hash(), LQPUniqueConstraint({_b}).hash());
  EXPECT_NE(unique_constraint_a_b_c.hash(), LQPUniqueConstraint({_a, _b}).hash());
  EXPECT_NE(unique_constraint_a_b_c.hash(), LQPUniqueConstraint({_a, _b, _c, _x}).hash());
}

}  // namespace opossum
