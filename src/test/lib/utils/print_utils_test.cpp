#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/print_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

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

  // Functor to access the MockNode's inputs.
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

  // Functor to print the MockNode's name.
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

TEST_F(PrintUtilsTest, print_expressions) {
  const auto mock_node = MockNode::make(MockNode::ColumnDefinitions{
      {DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}, {DataType::Int, "d"}});
  const auto a = mock_node->get_column("a");
  const auto b = mock_node->get_column("b");
  const auto c = mock_node->get_column("c");
  const auto d = mock_node->get_column("d");

  auto stream = std::stringstream{};
  auto expression_vec = expression_vector(equals_(c, a), avg_(b), d, 1);

  const auto expected_string = "c = a, AVG(b), d, 1";
  print_expressions(expression_vec, stream);
  EXPECT_EQ(stream.str(), expected_string);

  stream.str("");
  print_expressions(expression_vec, stream, " - ");
  // Separator is used.
  EXPECT_EQ(stream.str(), "c = a - AVG(b) - d - 1");

  for (const auto& expression_set : {ExpressionUnorderedSet{avg_(b), equals_(c, a), d, value_(1)},
                                     ExpressionUnorderedSet{equals_(c, a), avg_(b), d, value_(1)}}) {
    stream.str("");
    print_expressions(expression_set, stream);
    // Expressions are ordered by minimal found original ColumnID.
    EXPECT_EQ(stream.str(), expected_string);
  }
}

TEST_F(PrintUtilsTest, all_encoding_options) {
  EXPECT_EQ(all_encoding_options(), "Unencoded, Dictionary, RunLength, FixedStringDictionary, FrameOfReference, LZ4");
}

}  // namespace hyrise
