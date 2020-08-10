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

  auto unique_constraints = std::make_shared<LQPUniqueConstraints>();
  const auto predicates = join_predicates();
  if (predicates.empty() || predicates.size() > 1) {
    // No guarantees implemented yet for Cross Joins and multi-predicate joins
    return unique_constraints;
  }

  DebugAssert(join_mode == JoinMode::Inner || join_mode == JoinMode::Left || join_mode == JoinMode::Right ||
                  join_mode == JoinMode::FullOuter,
              "Unhandled JoinMode");

  const auto join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates().front());
  if (!join_predicate || join_predicate->predicate_condition != PredicateCondition::Equals) {
    // Also, no guarantees implemented yet for other join predicates than _equals() (Equi Join)
    return unique_constraints;
  }

  const auto& left_unique_constraints = left_input()->unique_constraints();
  const auto& right_unique_constraints = right_input()->unique_constraints();

  // Check uniqueness of join columns
  bool left_operand_is_unique = left_input()->has_matching_unique_constraint({join_predicate->left_operand()});
  bool right_operand_is_unique = right_input()->has_matching_unique_constraint({join_predicate->right_operand()});

  if (left_operand_is_unique && right_operand_is_unique) {
    // Due to the one-to-one relationship, the constraints of both sides remain valid.
    for (const auto& unique_constraint : *left_unique_constraints) {
      unique_constraints->emplace_back(unique_constraint);
    }
    for (const auto& unique_constraint : *right_unique_constraints) {
      unique_constraints->emplace_back(unique_constraint);
    }

  } else if (left_operand_is_unique) {
    // Uniqueness on the left prevents duplication of records on the right
    return right_unique_constraints;

  } else if (right_operand_is_unique) {
    // Uniqueness on the right prevents duplication of records on the left
    return left_unique_constraints;
  }

  return unique_constraints;
}

std::vector<FunctionalDependency> JoinNode::non_trivial_functional_dependencies() const {

  /**
   * Due to the logic of joins, we might lose several unique constraints in this node. In consequence, some FDs become
   * non-trivial and have to be returned by this function.
   */
  auto non_trivial_fds = _fds_from_unique_constraints(_discarded_unique_constraints());

  // Merge with child nodes' non-trivial FDs output vector
  auto non_trivial_fds_left = left_input()->non_trivial_functional_dependencies();
  auto fds_out = std::vector<FunctionalDependency>(non_trivial_fds.size() + non_trivial_fds_left.size());
  std::move(non_trivial_fds.begin(), non_trivial_fds.end(), std::back_inserter(fds_out));
  std::move(non_trivial_fds_left.begin(), non_trivial_fds_left.end(), std::back_inserter(fds_out));

  // We also merge the right table's FDs (except for Semi- & Anti-Joins)
  if(join_mode != JoinMode::Semi && join_mode != JoinMode::AntiNullAsFalse && join_mode != JoinMode::AntiNullAsTrue) {
    auto non_trivial_fds_right = left_input()->non_trivial_functional_dependencies();
    for(const auto& fd_right : non_trivial_fds_right) {
      // We do not want to add duplicate FDs
      if(std::none_of(non_trivial_fds_left.cbegin(), non_trivial_fds_left.cend(), [&fd_right](const auto& fd_left){
            return fd_left == fd_right;
          })) {
        fds_out.push_back(fd_right);
      }
    }
  }

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
