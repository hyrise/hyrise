#include "join_node.hpp"

#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "lqp_utils.hpp"
#include "operators/operator_join_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

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
  const auto output_both_inputs = !is_semi_or_anti_join(join_mode);
  if (!output_both_inputs) {
    return left_expressions;
  }

  const auto& right_expressions = right_input()->output_expressions();
  auto output_expressions = std::vector<std::shared_ptr<AbstractExpression>>{};
  output_expressions.resize(left_expressions.size() + right_expressions.size());

  auto right_begin = std::copy(left_expressions.begin(), left_expressions.end(), output_expressions.begin());
  std::copy(right_expressions.begin(), right_expressions.end(), right_begin);

  return output_expressions;
}

UniqueColumnCombinations JoinNode::unique_column_combinations() const {
  // We cannot guarantee any UCCs for Cross-Joins.
  if (join_mode == JoinMode::Cross) {
    return UniqueColumnCombinations{};
  }

  // Semi- and Anti-Joins act as mere filters for input_left(). Thus, existing unique column combinations remain valid.
  if (is_semi_or_anti_join(join_mode)) {
    return _forward_left_unique_column_combinations();
  }

  const auto& left_unique_column_combinations = left_input()->unique_column_combinations();
  const auto& right_unique_column_combinations = right_input()->unique_column_combinations();

  return _output_unique_column_combinations(left_unique_column_combinations, right_unique_column_combinations);
}

UniqueColumnCombinations JoinNode::_output_unique_column_combinations(
    const UniqueColumnCombinations& left_unique_column_combinations,
    const UniqueColumnCombinations& right_unique_column_combinations) const {
  if (left_unique_column_combinations.empty() && right_unique_column_combinations.empty()) {
    // Early exit.
    return UniqueColumnCombinations{};
  }

  const auto predicates = join_predicates();
  if (predicates.empty() || predicates.size() > 1) {
    // No guarantees implemented yet for Cross Joins and multi-predicate joins.
    return UniqueColumnCombinations{};
  }

  DebugAssert(join_mode == JoinMode::Inner || join_mode == JoinMode::Left || join_mode == JoinMode::Right ||
                  join_mode == JoinMode::FullOuter,
              "Unhandled JoinMode");

  const auto join_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicates().front());
  if (!join_predicate || join_predicate->predicate_condition != PredicateCondition::Equals) {
    // Also, no guarantees implemented yet for other join predicates than equals_() (Equi Join).
    return UniqueColumnCombinations{};
  }

  // Check uniqueness of join columns.
  const auto left_operand_is_unique =
      !left_unique_column_combinations.empty() &&
      contains_matching_unique_column_combination(left_unique_column_combinations, {join_predicate->left_operand()});
  const auto right_operand_is_unique =
      !right_unique_column_combinations.empty() &&
      contains_matching_unique_column_combination(right_unique_column_combinations, {join_predicate->right_operand()});

  if (left_operand_is_unique && right_operand_is_unique) {
    // Due to the one-to-one relationship, the UCCs of both sides remain valid.
    auto unique_column_combinations =
        UniqueColumnCombinations{left_unique_column_combinations.begin(), left_unique_column_combinations.end()};
    unique_column_combinations.insert(right_unique_column_combinations.cbegin(),
                                      right_unique_column_combinations.cend());
    return unique_column_combinations;
  }

  if (left_operand_is_unique) {
    // Uniqueness on the left prevents duplication of records on the right.
    return right_unique_column_combinations;
  }

  if (right_operand_is_unique) {
    // Uniqueness on the right prevents duplication of records on the left.
    return left_unique_column_combinations;
  }

  return UniqueColumnCombinations{};
}

FunctionalDependencies JoinNode::non_trivial_functional_dependencies() const {
  /**
   * In the case of Semi- & Anti-Joins, this node acts as a filter for the left input node. The number of output
   * expressions does not change and therefore we should forward non-trivial FDs as follows:
   */
  if (is_semi_or_anti_join(join_mode)) {
    return left_input()->non_trivial_functional_dependencies();
  }

  /**
   * When joining tables, we usually lose some or even all unique column combinations from both input tables. This leads
   * to fewer trivial FDs that we can generate from UCCs in upper nodes. To preserve all possible FDs, we manually
   * forward all FDs from input nodes with discarded UCCs.
   */
  auto fds_left = FunctionalDependencies{};
  auto fds_right = FunctionalDependencies{};

  const auto& left_unique_column_combinations = left_input()->unique_column_combinations();
  const auto& right_unique_column_combinations = right_input()->unique_column_combinations();
  const auto& output_unique_column_combinations =
      _output_unique_column_combinations(left_unique_column_combinations, right_unique_column_combinations);

  if (output_unique_column_combinations.empty() && !left_unique_column_combinations.empty() &&
      !right_unique_column_combinations.empty()) {
    // Left and right UCCs are discarded, so we have to manually forward all FDs from the input nodes.
    fds_left = left_input()->functional_dependencies();
    fds_right = right_input()->functional_dependencies();
  } else if ((output_unique_column_combinations.empty() ||
              output_unique_column_combinations == right_unique_column_combinations) &&
             !left_unique_column_combinations.empty()) {
    // Left UCCs are discarded, so we have to manually forward all FDs of the left input node.
    fds_left = left_input()->functional_dependencies();
    fds_right = right_input()->non_trivial_functional_dependencies();
  } else if ((output_unique_column_combinations.empty() ||
              output_unique_column_combinations == left_unique_column_combinations) &&
             !right_unique_column_combinations.empty()) {
    // Right UCCs are discarded, so we have to manually forward all FDs of the right input node.
    fds_left = left_input()->non_trivial_functional_dependencies();
    fds_right = right_input()->functional_dependencies();
  } else {
    // No UCCs are discarded. We only have to forward non-trivial FDs.
    DebugAssert(output_unique_column_combinations.size() ==
                    (left_unique_column_combinations.size() + right_unique_column_combinations.size()),
                "Unexpected number of unique constraints.");
    fds_left = left_input()->non_trivial_functional_dependencies();
    fds_right = right_input()->non_trivial_functional_dependencies();
  }

  // Prevent FDs with duplicate determinant expressions in the output vector.
  auto fds_out = union_fds(fds_left, fds_right);

  // Outer joins lead to nullable columns, which may invalidate some FDs.
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
  }

  const auto right_column_id =
      static_cast<ColumnID>(column_id - static_cast<ColumnID::base_type>(left_input_column_count));
  return right_input()->is_column_nullable(right_column_id);
}

const std::vector<std::shared_ptr<AbstractExpression>>& JoinNode::join_predicates() const {
  return node_expressions;
}

void JoinNode::mark_as_semi_reduction(const std::shared_ptr<JoinNode>& reduced_join_node) {
  Assert(!_is_semi_reduction, "The semi reduction status should be set once only.");
  Assert(reduced_join_node, "Reduced JoinNode must be provided.");
  Assert(join_mode == JoinMode::Semi, "Semi join reductions require JoinMode::Semi.");
  DebugAssert(join_predicates().size() == 1,
              "Currently, semi join reductions are expected to have a single join predicate.");
  DebugAssert(std::any_of(reduced_join_node->join_predicates().cbegin(), reduced_join_node->join_predicates().cend(),
                          [&](const auto predicate) { return *predicate == *join_predicates()[0]; }),
              "Both semi join reduction node and the reduced join should have a common join predicate.");
  _is_semi_reduction = true;
  _reduced_join_node = std::weak_ptr<JoinNode>(reduced_join_node);
}

void JoinNode::mark_input_side_as_prunable(LQPInputSide input_side) {
  _prunable_input_side = input_side;
}

bool JoinNode::is_semi_reduction() const {
  DebugAssert(!_is_semi_reduction || join_mode == JoinMode::Semi, "Non-semi join is marked as a semi reduction.");
  return _is_semi_reduction;
}

std::optional<LQPInputSide> JoinNode::prunable_input_side() const {
  if (is_semi_or_anti_join(join_mode)) {
    return LQPInputSide::Right;
  }
  return _prunable_input_side;
}

std::shared_ptr<JoinNode> JoinNode::get_or_find_reduced_join_node() const {
  Assert(_is_semi_reduction, "Expected semi join reduction node.");

  if (_reduced_join_node.expired()) {
    // In deep copies of the LQP, the weak pointer to the reduced join is unset (lazy discovery). In such cases,
    // find the reduced join by traversing the LQP upwards.
    const auto& reduction_predicate = *join_predicates()[0];
    visit_lqp_upwards(std::const_pointer_cast<AbstractLQPNode>(shared_from_this()), [&](const auto& current_node) {
      if (current_node->type != LQPNodeType::Join || current_node.get() == this) {
        return LQPUpwardVisitation::VisitOutputs;
      }
      const auto join_node = std::static_pointer_cast<JoinNode>(current_node);
      if (std::none_of(join_node->join_predicates().begin(), join_node->join_predicates().end(),
                       [&](const auto& predicate) { return *predicate == reduction_predicate; })) {
        return LQPUpwardVisitation::VisitOutputs;
      }

      _reduced_join_node = std::weak_ptr<JoinNode>(join_node);
      return LQPUpwardVisitation::DoNotVisitOutputs;
    });

    Assert(!_reduced_join_node.expired(), "Could not find JoinNode that gets reduced by this semi join reduction.");
  }

  return _reduced_join_node.lock();
}

size_t JoinNode::_on_shallow_hash() const {
  size_t hash = boost::hash_value(join_mode);
  boost::hash_combine(hash, _is_semi_reduction);
  return hash;
}

std::shared_ptr<AbstractLQPNode> JoinNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  if (join_predicates().empty()) {
    Assert(join_mode == JoinMode::Cross, "Expected cross join.");
    return JoinNode::make(join_mode);
  }
  const auto copied_join_node =
      JoinNode::make(join_mode, expressions_copy_and_adapt_to_different_lqp(join_predicates(), node_mapping));
  copied_join_node->_is_semi_reduction = _is_semi_reduction;
  return copied_join_node;
}

bool JoinNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& join_node = static_cast<const JoinNode&>(rhs);
  if (join_mode != join_node.join_mode || _is_semi_reduction != join_node._is_semi_reduction) {
    return false;
  }
  return expressions_equal_to_expressions_in_different_lqp(join_predicates(), join_node.join_predicates(),
                                                           node_mapping);
}

}  // namespace hyrise
