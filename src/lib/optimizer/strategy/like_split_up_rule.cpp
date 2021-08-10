#include "like_split_up_rule.hpp"

#include <algorithm>
#include <functional>
#include <unordered_set>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/like_matcher.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/alias_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

std::string LikeSplitUpRule::name() const {
  static const auto name = std::string{"LikeSplitUpRule"};
  return name;
}

void LikeSplitUpRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  Assert(lqp_root->type == LQPNodeType::Root, "ExpressionReductionRule needs root to hold onto");

  visit_lqp(lqp_root, [&](const auto& sub_node) {
    for (auto& expression : sub_node->node_expressions) {
      split_up_like(sub_node, expression);
    }

    return LQPVisitation::VisitInputs;
  });
}

void LikeSplitUpRule::split_up_like(std::shared_ptr<AbstractLQPNode> sub_node, std::shared_ptr<AbstractExpression>& input_expression) {
  // Continue only if the expression is a LIKE/NOT LIKE expression
  const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(input_expression);
  if (!binary_predicate) {
    // std::cout << 43 << std::endl;
    return;
  }
  if (binary_predicate->predicate_condition != PredicateCondition::Like) {
    // std::cout << 47 << std::endl;
    return;
  }

  // Continue only if right operand is a literal/value (expr LIKE 'asdf%')
  const auto pattern_value_expression = std::dynamic_pointer_cast<ValueExpression>(binary_predicate->right_operand());
  if (!pattern_value_expression) {
    // std::cout << 54 << std::endl;
    return;
  }

  const auto pattern = boost::get<pmr_string>(pattern_value_expression->value);

  // Continue only if the pattern ends with a "%"-wildcard, has a non-empty prefix and contains no other wildcards
  const auto single_char_wildcard_pos = pattern.find_first_of('_');

  if (single_char_wildcard_pos != pmr_string::npos) {
    // std::cout << 64 << std::endl;
    return;
  }

  const auto multi_char_wildcard_pos = pattern.find_first_of('%');
  // TODO(anyone): we do not rewrite LIKEs with multiple wildcards here. Theoretically, we could rewrite "c LIKE RED%E%"
  // to "c >= RED and C < REE and c LIKE RED%E%" but that would require adding new PredicateNodes. For now, we assume
  // that the potential pruning of such LIKE predicates via the ChunkPruningRule is sufficient. However, if not many
  // chunks can be pruned, rewriting with additional predicates might show to be beneficial.
  if (multi_char_wildcard_pos == std::string::npos || multi_char_wildcard_pos == 0 || std::count(pattern.cbegin(), pattern.cend(), '%') != 2) {
    // std::cout << 74 << std::endl;
    return;
  }

  const auto first_pattern = pattern.substr(0, multi_char_wildcard_pos) + "%";
  const auto second_pattern = pattern.substr(multi_char_wildcard_pos);

  const auto new_1 = PredicateNode::make(std::make_shared<BinaryPredicateExpression>(PredicateCondition::Like, binary_predicate->left_operand(), std::make_shared<ValueExpression>(first_pattern)));
  const auto new_2 = PredicateNode::make(std::make_shared<BinaryPredicateExpression>(PredicateCondition::Like, binary_predicate->left_operand(), std::make_shared<ValueExpression>(second_pattern)));

  // std::cout << "Found for " << pattern << std::endl;

  lqp_replace_node(sub_node, new_1);
  lqp_insert_node(new_1, LQPInputSide::Left, new_2);

  // PredicateNode::make(like_());

  // lqp_remove_node

  // const auto bounds = LikeMatcher::bounds(pattern);

  // // In case of an ASCII overflow
  // if (!bounds) return;

  // const auto [lower_bound, upper_bound] = *bounds;

  // if (binary_predicate->predicate_condition == PredicateCondition::Like) {
  //   input_expression = between_upper_exclusive_(binary_predicate->left_operand(), lower_bound, upper_bound);
  // } else {  // binary_predicate->predicate_condition == PredicateCondition::NotLike
  //   input_expression = or_(less_than_(binary_predicate->left_operand(), lower_bound),
  //                          greater_than_equals_(binary_predicate->left_operand(), upper_bound));
  // }
}

}  // namespace opossum
