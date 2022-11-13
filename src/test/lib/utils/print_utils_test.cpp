#include "base_test.hpp"

#include "logical_query_plan/mock_node.hpp"
#include "storage/table.hpp"
#include "utils/print_utils.hpp"

namespace hyrise {

class PrintUtilsTest : public BaseTest {};

TEST_F(PrintUtilsTest, print_directed_acyclic_graph) {
  const auto column_definitions = MockNode::ColumnDefinitions{};
  const auto recurring_node = MockNode::make(column_definitions, "recurring_node");
  // clang-format off
  const auto lqp =
  MockNode::make(column_definitions, "node_a",
    MockNode::make(column_definitions, "node_b",
      recurring_node),
    MockNode::make(column_definitions, "node_c",
      recurring_node,
      recurring_node));
  // clang-format on

  // Functor to access the MockNode's inputs
  const auto get_inputs_fn = [](const auto& node) {
    std::vector<std::shared_ptr<const AbstractLQPNode>> inputs;
    if (node->left_input()) {
      inputs.emplace_back(node->left_input());
    }

    if (node->right_input()) {
      inputs.emplace_back(node->right_input());
    }

    return inputs;
  };

  // Functor to print the MockNode's name
  const auto node_print_fn = [](const auto& node, auto& stream) {
    const auto& mock_node = static_cast<const MockNode&>(*node);
    stream << *mock_node.name;
  };

  auto stream = std::ostringstream{};
  const auto expected_string = std::string{
      R"([0] node_a
 \_[1] node_b
 |  \_[2] recurring_node
 \_[3] node_c
    \_Recurring Node --> [2]
    \_Recurring Node --> [2]
)"};

  print_directed_acyclic_graph<const AbstractLQPNode>(lqp, get_inputs_fn, node_print_fn, stream);
  EXPECT_EQ(stream.str(), expected_string);
}

TEST_F(PrintUtilsTest, print_table_key_constraints) {
  const auto table = Table::create_dummy_table(
      {{"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}});
  auto stream = std::ostringstream{};

  print_table_key_constraints(table, stream);
  // Empty constraints do not print anything.
  EXPECT_EQ(stream.str(), "");

  stream.str("");
  table->add_soft_key_constraint({{ColumnID{1}, ColumnID{0}}, KeyConstraintType::UNIQUE});
  table->add_soft_key_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});

  print_table_key_constraints(table, stream);
  // Primary keys are printed before unique constraints.
  EXPECT_EQ(stream.str(), "PRIMARY_KEY(c), UNIQUE(a, b)");

  stream.str("");
  print_table_key_constraints(table, stream, " | ");
  // Separator is used.
  EXPECT_EQ(stream.str(), "PRIMARY_KEY(c) | UNIQUE(a, b)");
}

}  // namespace hyrise
