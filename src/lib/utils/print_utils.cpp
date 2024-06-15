#include "print_utils.hpp"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <memory>
#include <ostream>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "magic_enum.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/encoding_type.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 *
 * @param indentation   Helper structure for the indentation of a node. Its size determines the level of indentation.
 *                      For each position, `true` means a vertical line "|", and `false` means a space " " should be
 *                      used to increase the indentation.
 * @param id_by_node    Mapping of nodes to their IDs. Used to determine whether a node was already printed and which ID
 *                      it had.
 * @param id_counter    ID of the last node. Used to generate the ID of the current node.
 */
template <typename Node>
void print_directed_acyclic_graph_impl(const std::shared_ptr<Node>& node,
                                       const NodeGetChildrenFn<Node>& get_children_fn,
                                       const NodePrintFn<Node>& node_print_fn, std::ostream& stream,
                                       std::vector<bool>& indentation,
                                       std::unordered_map<std::shared_ptr<const Node>, size_t>& id_by_node,
                                       size_t& id_counter) {
  // Indent whilst drawing the edges.
  const auto max_indentation = indentation.empty() ? 0 : indentation.size() - 1;
  for (size_t level = 0; level < max_indentation; ++level) {
    if (indentation[level]) {
      stream << " | ";
    } else {
      stream << "   ";
    }
  }

  // Only the root node is not "pointed at" with "\_<node_info>".
  if (!indentation.empty()) {
    stream << " \\_";
  }

  // Check whether the node has been printed before.
  const auto iter = id_by_node.find(node);
  if (iter != id_by_node.end()) {
    stream << "Recurring Node --> [" << iter->second << "]\n";
    return;
  }

  const auto this_node_id = id_counter;
  id_counter++;
  id_by_node.emplace(node, this_node_id);

  // Print node info.
  stream << "[" << this_node_id << "] ";
  node_print_fn(node, stream);
  stream << '\n';

  const auto children = get_children_fn(node);
  indentation.emplace_back(true);

  // Recursively progress to children.
  for (size_t child_idx = 0; child_idx < children.size(); ++child_idx) {
    if (child_idx + 1 == children.size()) {
      indentation.back() = false;
    }
    print_directed_acyclic_graph_impl<Node>(children[child_idx], get_children_fn, node_print_fn, stream, indentation,
                                            id_by_node, id_counter);
  }

  indentation.pop_back();
}

}  // namespace

namespace hyrise {

template <typename Node>
void print_directed_acyclic_graph(const std::shared_ptr<Node>& node, const NodeGetChildrenFn<Node>& get_children_fn,
                                  const NodePrintFn<Node>& print_node_fn, std::ostream& stream) {
  auto levels = std::vector<bool>{};
  auto id_by_node = std::unordered_map<std::shared_ptr<Node>, size_t>{};
  auto id_counter = size_t{0};

  print_directed_acyclic_graph_impl<Node>(node, get_children_fn, print_node_fn, stream, levels, id_by_node, id_counter);
}

// We explicitly instantiate these template functions because clang-12(+) does not instantiate them for us.
template void print_directed_acyclic_graph<const AbstractLQPNode>(
    const std::shared_ptr<const AbstractLQPNode>& node, const NodeGetChildrenFn<const AbstractLQPNode>& get_children_fn,
    const NodePrintFn<const AbstractLQPNode>& print_node_fn, std::ostream& stream);
template void print_directed_acyclic_graph<const AbstractOperator>(
    const std::shared_ptr<const AbstractOperator>& node,
    const NodeGetChildrenFn<const AbstractOperator>& get_children_fn,
    const NodePrintFn<const AbstractOperator>& print_node_fn, std::ostream& stream);

void print_table_key_constraints(const std::shared_ptr<const Table>& table, std::ostream& stream,
                                 const std::string& separator) {
  const auto& table_key_constraints =
      std::set<TableKeyConstraint>{table->soft_key_constraints().cbegin(), table->soft_key_constraints().cend()};
  if (table_key_constraints.empty()) {
    return;
  }

  const auto& last_constraint = *std::prev(table_key_constraints.cend());
  for (const auto& constraint : table_key_constraints) {
    stream << magic_enum::enum_name(constraint.key_type()) << "(";
    const auto& columns = constraint.columns();
    Assert(!columns.empty(), "Did not expect useless constraint.");
    const auto& last_column = *std::prev(columns.cend());
    for (auto column : columns) {
      stream << table->column_name(column);
      if (column != last_column) {
        stream << ", ";
      }
    }
    stream << ")";
    if (constraint != last_constraint) {
      stream << separator;
    }
  }
}

void print_expressions(const ExpressionUnorderedSet& expressions, std::ostream& stream, const std::string& separator) {
  auto expressions_vector = std::vector<std::shared_ptr<AbstractExpression>>{expressions.begin(), expressions.end()};

  // Obtain minimal ColumnID found in expression.
  const auto minimal_column_id = [](const auto& expression) {
    // Initialize with maximum value to print expressions where we cannot get the original ColumnID last.
    auto column_id = INVALID_COLUMN_ID;

    visit_expression(expression, [&](const auto& current_expression) {
      if (current_expression->type != ExpressionType::LQPColumn) {
        return ExpressionVisitation::VisitArguments;
      }

      column_id = std::min(column_id, static_cast<const LQPColumnExpression&>(*current_expression).original_column_id);
      return ExpressionVisitation::DoNotVisitArguments;
    });
    return column_id;
  };

  // Sort by ColumnID.
  std::sort(expressions_vector.begin(), expressions_vector.end(), [&](const auto& lhs, const auto& rhs) {
    return minimal_column_id(lhs) < minimal_column_id(rhs);
  });

  print_expressions(expressions_vector, stream, separator);
}

void print_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions, std::ostream& stream,
                       const std::string& separator) {
  DebugAssert(!expressions.empty(), "Did not expect empty expression vector.");
  stream << expressions[0]->as_column_name();
  const auto expression_count = expressions.size();
  for (auto expression_idx = size_t{1}; expression_idx < expression_count; ++expression_idx) {
    stream << separator << expressions[expression_idx]->as_column_name();
  }
}

std::string all_encoding_options() {
  return boost::algorithm::join(
      magic_enum::enum_names<EncodingType>() | boost::adaptors::transformed([](const auto& it) {
        return std::string{it};
      }),
      ", ");
}

}  // namespace hyrise
