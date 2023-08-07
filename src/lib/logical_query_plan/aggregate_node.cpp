#include "aggregate_node.hpp"

#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "lqp_utils.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {
using NodeExpressionsDifferenceType =
    typename std::iterator_traits<decltype(hyrise::AggregateNode::node_expressions)::iterator>::difference_type;
}  // namespace

namespace hyrise {

AggregateNode::AggregateNode(const std::vector<std::shared_ptr<AbstractExpression>>& group_by_expressions,
                             const std::vector<std::shared_ptr<AbstractExpression>>& aggregate_expressions)
    : AbstractLQPNode(LQPNodeType::Aggregate, {/* Expressions added below*/}),
      aggregate_expressions_begin_idx{group_by_expressions.size()} {
  if constexpr (HYRISE_DEBUG) {
    for (const auto& aggregate_expression : aggregate_expressions) {
      Assert(aggregate_expression->type == ExpressionType::WindowFunction,
             "Expression used as aggregate expression must be of type WindowFunctionExpression.");
      const auto& window_function = static_cast<const WindowFunctionExpression&>(*aggregate_expression);
      Assert(!window_function.window(), "Aggregates must not define a window.");
      Assert(
          aggregate_functions.contains(window_function.window_function),
          window_function_to_string.left.at(window_function.window_function) + " is not a valid aggregate function.");
    }
  }

  node_expressions.resize(group_by_expressions.size() + aggregate_expressions.size());
  std::copy(group_by_expressions.begin(), group_by_expressions.end(), node_expressions.begin());
  std::copy(aggregate_expressions.begin(), aggregate_expressions.end(),
            node_expressions.begin() + static_cast<NodeExpressionsDifferenceType>(group_by_expressions.size()));
}

std::string AggregateNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);
  std::stringstream stream;

  stream << "[Aggregate] ";

  stream << "GroupBy: [";
  for (auto expression_idx = ColumnID{0}; expression_idx < aggregate_expressions_begin_idx; ++expression_idx) {
    stream << node_expressions[expression_idx]->description(expression_mode);
    if (expression_idx + 1u < aggregate_expressions_begin_idx) {
      stream << ", ";
    }
  }
  stream << "] ";

  stream << "Aggregates: [";
  for (auto expression_idx = aggregate_expressions_begin_idx; expression_idx < node_expressions.size();
       ++expression_idx) {
    stream << node_expressions[expression_idx]->description(expression_mode);
    if (expression_idx + 1 < node_expressions.size()) {
      stream << ", ";
    }
  }
  stream << "]";

  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> AggregateNode::output_expressions() const {
  // We do not return node_expressions directly here, because we do not want to expose ANY() to the following LQP
  // nodes. This way, we execute ANY() as intended, but do not have to traverse the LQP upwards and adapt nodes
  // that reference the ANY'd column.
  auto output_expressions = node_expressions;

  const auto output_expression_count = output_expressions.size();
  for (auto expression_idx = aggregate_expressions_begin_idx; expression_idx < output_expression_count;
       ++expression_idx) {
    auto& output_expression = output_expressions[expression_idx];
    const auto& aggregate_expression = static_cast<WindowFunctionExpression&>(*output_expression);
    if (aggregate_expression.window_function == WindowFunction::Any) {
      output_expression = output_expression->arguments[0];
    }
  }

  return output_expressions;
}

bool AggregateNode::is_column_nullable(const ColumnID column_id) const {
  Assert(column_id < node_expressions.size(), "ColumnID out of range");
  Assert(left_input(), "Need left input to determine nullability");
  return node_expressions[column_id]->is_nullable_on_lqp(*left_input());
}

UniqueColumnCombinations AggregateNode::unique_column_combinations() const {
  auto unique_column_combinations = UniqueColumnCombinations{};

  /**
   * (1) Forward unique column combinations from child nodes if all expressions belong to the group-by section.
   *     Note: The DependentGroupByReductionRule might wrap some expressions with an ANY() aggregate function.
   *     However, ANY() is a pseudo aggregate function that does not change any values.
   *     (cf. DependentGroupByReductionRule)
   *     Therefore, ANY()-wrapped columns can be interpreted as group-by columns.
   *
   *     Future Work:
   *     Some aggregation functions maintain the uniqueness of their input expressions. For example, if {a} is unique,
   *     so is MAX(a), independently of the group by columns. We could create these new UCCs as shown in the following
   *     example:
   *
   *     Consider a StoredTableNode with the column expressions {a, b, c, d} and two unique column combinations:
   *       - UCC for {a, c}.
   *       - UCC for {b, d}.
   *     An AggregateNode which follows defines the following:
   *       - COUNT(a), MAX(b)
   *       - Group By {c, d}
   *     => The UCC for {a, c} has to be discarded because of the COUNT(a) aggregate.
   *     => The UCC for {b, d} can be reformulated as { MAX(b), d }
   *
   *     Furthermore, for AggregateNodes without group by columns, where only one row is generated, all columns are
   *     unique. We are not yet sure if this should be modeled as a UCCs.
   */

  // Check each UCC for applicability.
  const auto& output_expressions = this->output_expressions();
  const auto& input_unique_column_combinations = left_input()->unique_column_combinations();
  for (const auto& input_unique_constraint : input_unique_column_combinations) {
    if (!contains_all_expressions(input_unique_constraint.expressions, output_expressions)) {
      continue;
    }

    // Forward UCC.
    unique_column_combinations.emplace(input_unique_constraint);
  }

  // (2) Create a new UCC from the group-by column(s), which form a candidate key for the output relation.
  const auto group_by_columns_count = aggregate_expressions_begin_idx;
  if (group_by_columns_count > 0) {
    auto group_by_columns = ExpressionUnorderedSet{group_by_columns_count};
    std::copy_n(node_expressions.begin(), group_by_columns_count,
                std::inserter(group_by_columns, group_by_columns.begin()));

    // Make sure that we do not add an already existing or a superset UCC.
    if (unique_column_combinations.empty() ||
        !contains_matching_unique_column_combination(unique_column_combinations, group_by_columns)) {
      unique_column_combinations.emplace(group_by_columns);
    }
  }

  /**
   * Future Work:
   * Under some circumstances, the DependentGroupByReductionRule reduces the number of group-by columns. Consequently,
   * we might be able to create shorter unique column combinations after the optimizer rule has been run (shorter UCCs
   * are always preferred). However, it would be great if this function could return the shortest UCCs possible, without
   * having to rely on the execution of the optimizer rule. Fortunately, we can shorten UCCs ourselves by looking at the
   * available functional dependencies.
   * See the following discussion: https://github.com/hyrise/hyrise/pull/2156#discussion_r453220838.
   */

  return unique_column_combinations;
}

FunctionalDependencies AggregateNode::non_trivial_functional_dependencies() const {
  auto non_trivial_fds = left_input()->non_trivial_functional_dependencies();

  // In AggregateNode, some expressions get wrapped inside of WindowFunctionExpressions. Therefore, we have to discard
  // all FDs whose expressions are no longer part of the node's output expressions.
  remove_invalid_fds(shared_from_this(), non_trivial_fds);

  return non_trivial_fds;
}

size_t AggregateNode::_on_shallow_hash() const {
  return aggregate_expressions_begin_idx;
}

std::shared_ptr<AbstractLQPNode> AggregateNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  const auto group_by_expressions = std::vector<std::shared_ptr<AbstractExpression>>{
      node_expressions.begin(),
      node_expressions.begin() + static_cast<NodeExpressionsDifferenceType>(aggregate_expressions_begin_idx)};

  const auto aggregate_expressions = std::vector<std::shared_ptr<AbstractExpression>>{
      node_expressions.begin() + static_cast<NodeExpressionsDifferenceType>(aggregate_expressions_begin_idx),
      node_expressions.end()};

  return std::make_shared<AggregateNode>(
      expressions_copy_and_adapt_to_different_lqp(group_by_expressions, node_mapping),
      expressions_copy_and_adapt_to_different_lqp(aggregate_expressions, node_mapping));
}

bool AggregateNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& aggregate_node = static_cast<const AggregateNode&>(rhs);

  return expressions_equal_to_expressions_in_different_lqp(node_expressions, aggregate_node.node_expressions,
                                                           node_mapping) &&
         aggregate_expressions_begin_idx == aggregate_node.aggregate_expressions_begin_idx;
}
}  // namespace hyrise
