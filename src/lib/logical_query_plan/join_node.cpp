#include "join_node.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "lqp_utils.hpp"
#include "operators/operator_join_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

JoinNode::JoinNode(const JoinMode init_join_mode) : AbstractLQPNode(LQPNodeType::Join), join_mode(init_join_mode) {
  Assert(join_mode == JoinMode::Cross, "Only Cross Joins can be constructed without predicate");
}

JoinNode::JoinNode(const JoinMode init_join_mode, const std::shared_ptr<AbstractExpression>& join_predicate)
    : JoinNode(init_join_mode, std::vector<std::shared_ptr<AbstractExpression>>{join_predicate}) {}

JoinNode::JoinNode(const JoinMode init_join_mode,
                   const std::vector<std::shared_ptr<AbstractExpression>>& init_join_predicates)
    : AbstractLQPNode(LQPNodeType::Join, init_join_predicates), join_mode(init_join_mode) {
  Assert(join_mode != JoinMode::Cross, "Cross Joins take no predicate");
  Assert(!join_predicates().empty(), "Non-Cross Joins require predicates");
}

std::string JoinNode::description(const DescriptionMode mode) const {
  const auto expression_mode = _expression_description_mode(mode);

  std::stringstream stream;
  stream << "[Join] Mode: " << join_mode;

  for (const auto& predicate : join_predicates()) {
    stream << " [" << predicate->description(expression_mode) << "]";
  }

  return stream.str();
}

std::vector<std::shared_ptr<AbstractExpression>> JoinNode::output_expressions() const {
  Assert(left_input() && right_input(), "Both inputs need to be set to determine a JoinNode's output expressions");

  /**
   * Update the JoinNode's output expressions every time they are requested. An overhead, but keeps the LQP code simple.
   * Previously we propagated _input_changed() calls through the LQP every time a node changed and that required a lot
   * of feeble code.
   */

  const auto& left_expressions = left_input()->output_expressions();
  const auto& right_expressions = right_input()->output_expressions();

  const auto output_both_inputs =
      join_mode != JoinMode::Semi && join_mode != JoinMode::AntiNullAsTrue && join_mode != JoinMode::AntiNullAsFalse;

  auto output_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  output_expressions.resize(left_expressions.size() + (output_both_inputs ? right_expressions.size() : 0));

  auto right_begin = std::copy(left_expressions.begin(), left_expressions.end(), output_expressions.begin());

  if (output_both_inputs) std::copy(right_expressions.begin(), right_expressions.end(), right_begin);

  return output_expressions;
}

std::shared_ptr<LQPUniqueConstraints> JoinNode::unique_constraints() const {
  // Semi- and Anti-Joins act as mere filters for input_left().
  // Therefore, existing unique constraints remain valid.
  if (join_mode == JoinMode::Semi || join_mode == JoinMode::AntiNullAsTrue || join_mode == JoinMode::AntiNullAsFalse) {
    return _forward_left_unique_constraints();
  }

  const auto& left_unique_constraints = left_input()->unique_constraints();
  const auto& right_unique_constraints = right_input()->unique_constraints();

  return _output_unique_constraints(left_unique_constraints, right_unique_constraints);
}

std::shared_ptr<LQPUniqueConstraints> JoinNode::_output_unique_constraints(
    const std::shared_ptr<LQPUniqueConstraints>& left_unique_constraints,
    const std::shared_ptr<LQPUniqueConstraints>& right_unique_constraints) const {
  if (left_unique_constraints->empty() && right_unique_constraints->empty()) {
    // Early exit
    return std::make_shared<LQPUniqueConstraints>();
  }

  const auto predicates = join_predicates();
  if (predicates.empty() || predicates.size() > 1) {
    // No guarantees implemented yet for Cross Joins and multi-predicate joins
    return std::make_shared<LQPUniqueConstraints>();
  }

  DebugAssert(join_mode == JoinMode::Inner || join_mode == JoinMode::Left || join_mode == JoinMode::Right ||
                  join_mode == JoinMode::FullOuter,
              "Unhandled JoinMode");

  const auto join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates().front());
  if (!join_predicate || join_predicate->predicate_condition != PredicateCondition::Equals) {
    // Also, no guarantees implemented yet for other join predicates than _equals() (Equi Join)
    return std::make_shared<LQPUniqueConstraints>();
  }

  // Check uniqueness of join columns
  bool left_operand_is_unique =
      !left_unique_constraints->empty() &&
      contains_matching_unique_constraint(left_unique_constraints, {join_predicate->left_operand()});
  bool right_operand_is_unique =
      !right_unique_constraints->empty() &&
      contains_matching_unique_constraint(right_unique_constraints, {join_predicate->right_operand()});

  if (left_operand_is_unique && right_operand_is_unique) {
    // Due to the one-to-one relationship, the constraints of both sides remain valid.
    auto unique_constraints =
        std::make_shared<LQPUniqueConstraints>(left_unique_constraints->begin(), left_unique_constraints->end());
    std::copy(right_unique_constraints->begin(), right_unique_constraints->end(),
              std::back_inserter(*unique_constraints));
    return unique_constraints;

  } else if (left_operand_is_unique) {
    // Uniqueness on the left prevents duplication of records on the right
    return right_unique_constraints;
  } else if (right_operand_is_unique) {
    // Uniqueness on the right prevents duplication of records on the left
    return left_unique_constraints;
  }

  return std::make_shared<LQPUniqueConstraints>();
}

std::vector<FunctionalDependency> JoinNode::non_trivial_functional_dependencies() const {
  /**
   * In the case of Semi- & Anti-Joins, this node acts as a filter for the left input node. The number of output
   * expressions does not change and therefore we should forward non-trivial FDs as follows:
   */
  if (join_mode == JoinMode::Semi || join_mode == JoinMode::AntiNullAsTrue || join_mode == JoinMode::AntiNullAsFalse) {
    return left_input()->non_trivial_functional_dependencies();
  }

  /**
   * When joining tables, we usually lose some or even all unique constraints from both input tables. This leads to
   * fewer trivial FDs that we can generate from unique constraints in upper nodes.
   * To preserve all FDs possible, we manually forward all FDs from input nodes, which unique constraints become
   * discarded.
   */
  auto fds_left = std::vector<FunctionalDependency>();
  auto fds_right = std::vector<FunctionalDependency>();

  const auto left_unique_constraints = left_input()->unique_constraints();
  const auto right_unique_constraints = right_input()->unique_constraints();
  const auto& output_unique_constraints = _output_unique_constraints(left_unique_constraints, right_unique_constraints);

  if (output_unique_constraints->empty() && !left_unique_constraints->empty() && !right_unique_constraints->empty()) {
    // Left and Right unique constraints become discarded, so we have to manually forward all FDs from the input nodes.
    fds_left = left_input()->functional_dependencies();
    fds_right = right_input()->functional_dependencies();
  } else if ((output_unique_constraints->empty() || output_unique_constraints == right_unique_constraints) &&
             !left_unique_constraints->empty()) {
    // Left unique constraints become discarded, so we have to manually forward all left input node's FDs
    fds_left = left_input()->functional_dependencies();
    fds_right = right_input()->non_trivial_functional_dependencies();
  } else if ((output_unique_constraints->empty() || output_unique_constraints == left_unique_constraints) &&
             !right_unique_constraints->empty()) {
    // Right unique constraints become discarded, so we have to manually forward all right input node's FDs
    fds_left = left_input()->non_trivial_functional_dependencies();
    fds_right = right_input()->functional_dependencies();
  } else {
    // No unique constraints become discarded. We only have to forward non-trivial FDs.
    DebugAssert(
        output_unique_constraints->size() == (left_unique_constraints->size() + right_unique_constraints->size()),
        "Unexpected number of unique constraints.");
    fds_left = left_input()->non_trivial_functional_dependencies();
    fds_right = right_input()->non_trivial_functional_dependencies();
  }

  // Prevent FDs with duplicate determinant expressions in the output vector
  auto fds_out = union_fds(fds_left, fds_right);

  // Outer joins lead to nullable columns, which may invalidate some FDs
  if (!fds_out.empty() &&
      (join_mode == JoinMode::FullOuter || join_mode == JoinMode::Left || join_mode == JoinMode::Right)) {
    remove_invalid_fds(shared_from_this(), fds_out);
  }

  /**
   * Future Work: In some cases, it is possible to create FDs from the join columns.
   *              For example: a) {join_column_a} => {join_column_b}
   *                           b) {join_column_b} => {join_column_a}
   */

  return fds_out;
}

bool JoinNode::is_column_nullable(const ColumnID column_id) const {
  Assert(left_input() && right_input(), "Need both inputs to determine nullability");

  const auto left_input_column_count = left_input()->output_expressions().size();
  const auto column_is_from_left_input = column_id < left_input_column_count;

  if (join_mode == JoinMode::Left && !column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::Right && column_is_from_left_input) {
    return true;
  }

  if (join_mode == JoinMode::FullOuter) {
    return true;
  }

  if (column_is_from_left_input) {
    return left_input()->is_column_nullable(column_id);
  } else {
    ColumnID right_column_id =
        static_cast<ColumnID>(column_id - static_cast<ColumnID::base_type>(left_input_column_count));
    return right_input()->is_column_nullable(right_column_id);
  }
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::join_predicates() const { return node_expressions; }

size_t JoinNode::_on_shallow_hash() const { return boost::hash_value(join_mode); }

std::shared_ptr<AbstractLQPNode> JoinNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  if (!join_predicates().empty()) {
    return JoinNode::make(join_mode, expressions_copy_and_adapt_to_different_lqp(join_predicates(), node_mapping));
  } else {
    return JoinNode::make(join_mode);
  }
}

bool JoinNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& join_node = static_cast<const JoinNode&>(rhs);
  if (join_mode != join_node.join_mode) return false;
  return expressions_equal_to_expressions_in_different_lqp(join_predicates(), join_node.join_predicates(),
                                                           node_mapping);
}

}  // namespace opossum
