#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/window_node.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "utils/data_dependency_test_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class WindowNodeTest : public BaseTest {
 public:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");

    auto frame_description = FrameDescription{FrameType::Range, FrameBound{0, FrameBoundType::Preceding, true},
                                              FrameBound{0, FrameBoundType::CurrentRow, false}};
    _window = window_(expression_vector(), expression_vector(), std::vector<SortMode>{}, std::move(frame_description));
    _window_function = min_(_a, _window);

    _window_node = WindowNode::make(_window_function, _mock_node);
  }

 protected:
  std::shared_ptr<LQPColumnExpression> _a;
  std::shared_ptr<LQPColumnExpression> _b;
  std::shared_ptr<MockNode> _mock_node;

  std::shared_ptr<WindowExpression> _window;
  std::shared_ptr<WindowFunctionExpression> _window_function;

  std::shared_ptr<WindowNode> _window_node;
};

TEST_F(WindowNodeTest, InvalidExpressions) {
  EXPECT_THROW(avg_(_a, _b), std::logic_error);
  auto frame_description = _window->frame_description;
  EXPECT_THROW(
      window_(expression_vector(), expression_vector(_a), std::vector<SortMode>{}, std::move(frame_description)),
      std::logic_error);

  if constexpr (HYRISE_DEBUG) {
    EXPECT_THROW(WindowNode::make(std::shared_ptr<AbstractExpression>{}), std::logic_error);
    EXPECT_THROW(WindowNode::make(add_(_a, _b)), std::logic_error);
    EXPECT_THROW(WindowNode::make(avg_(_a)), std::logic_error);
  }
}

TEST_F(WindowNodeTest, NodeExpressions) {
  ASSERT_EQ(_window_node->node_expressions.size(), 1);
  EXPECT_EQ(_window_node->node_expressions.at(0), _window_function);
}

TEST_F(WindowNodeTest, Description) {
  EXPECT_EQ(_window_node->description(), "[Window] MIN(a) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)");
  auto frame_description = FrameDescription{FrameType::Rows, FrameBound{1, FrameBoundType::Preceding, false},
                                            FrameBound{2, FrameBoundType::Following, false}};
  auto window = window_(expression_vector(_a), expression_vector(_b), std::vector<SortMode>{SortMode::Descending},
                        std::move(frame_description));
  auto window_node = WindowNode::make(rank_(window));
  EXPECT_EQ(window_node->description(),
            "[Window] RANK() OVER (PARTITION BY a ORDER BY b Descending ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING)");
  frame_description = FrameDescription{FrameType::Rows, FrameBound{0, FrameBoundType::CurrentRow, false},
                                       FrameBound{0, FrameBoundType::Following, true}};
  window = window_(expression_vector(), expression_vector(), std::vector<SortMode>{}, std::move(frame_description));
  window_node = WindowNode::make(cume_dist_(window));
  EXPECT_EQ(window_node->description(), "[Window] CUME_DIST() OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)");
}

TEST_F(WindowNodeTest, ShallowEqualsAndCopy) {
  const auto node_copy = _window_node->deep_copy();
  const auto node_mapping = lqp_create_node_mapping(_window_node, node_copy);

  EXPECT_TRUE(_window_node->shallow_equals(*node_copy, node_mapping));
}

TEST_F(WindowNodeTest, OutputExpressions) {
  const auto& output_expressions = _window_node->output_expressions();
  ASSERT_EQ(output_expressions.size(), 3);
  ASSERT_EQ(*output_expressions[0], *_a);
  ASSERT_EQ(*output_expressions[1], *_b);
  ASSERT_EQ(*output_expressions[2], *_window_function);
}

TEST_F(WindowNodeTest, HashingAndEqualityCheck) {
  const auto node_copy = _window_node->deep_copy();
  EXPECT_EQ(*node_copy, *_window_node);

  const auto window_node_different_function = WindowNode::make(rank_(_window), _mock_node);
  EXPECT_NE(*window_node_different_function, *_window_node);

  auto frame_description = _window->frame_description;
  const auto window =
      window_(expression_vector(_a), expression_vector(), std::vector<SortMode>{}, std::move(frame_description));
  const auto window_node_different_window = WindowNode::make(rank_(window), _mock_node);
  EXPECT_NE(*window_node_different_window, *_window_node);
  EXPECT_NE(*window_node_different_window, *window_node_different_function);

  EXPECT_EQ(node_copy->hash(), _window_node->hash());
  EXPECT_NE(window_node_different_function->hash(), _window_node->hash());
  EXPECT_NE(window_node_different_window->hash(), _window_node->hash());
}

TEST_F(WindowNodeTest, UniqueColumnCombinationsEmpty) {
  EXPECT_TRUE(_mock_node->unique_column_combinations().empty());
  EXPECT_TRUE(_window_node->unique_column_combinations().empty());
}

TEST_F(WindowNodeTest, UniqueColumnCombinationsForwarding) {
  // Add constraints to MockNode.
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({key_constraint_a_b, key_constraint_b});

  // Basic check.
  const auto& unique_column_combinations = _window_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 2);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_a_b, unique_column_combinations));
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_b, unique_column_combinations));
}

}  // namespace hyrise
