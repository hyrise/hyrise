#include "simplify_expression_rule.hpp"

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/predicate_node.hpp"
#include "optimizer/expression/expression_node.hpp"

namespace opossum {

std::shared_ptr<AbstractASTNode> SimplifyExpressionRule::apply_rule(std::shared_ptr<AbstractASTNode> node) {
  // For now we just optimize expressions in TableScans
  if (node->left_child()) apply_rule(node->left_child());
  if (node->right_child()) apply_rule(node->right_child());

  if (node->type() == ASTNodeType::Predicate) {
    auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
    auto predicate = predicate_node->predicate();
    auto optimized = simplify_addition_of_literals(predicate);
    predicate_node->set_predicate(optimized);
  }

  return node;
}

std::shared_ptr<ExpressionNode> SimplifyExpressionRule::simplify_addition_of_literals(
    std::shared_ptr<ExpressionNode> node) {
  if (node->left_child() && node->right_child()) {
    if (node->type() == ExpressionType::Plus && node->left_child()->type() == ExpressionType::Literal &&
        node->right_child()->type() == ExpressionType::Literal) {
      auto left_child = node->left_child();
      auto right_child = node->right_child();

      return node;
      //   return std::make_shared<ExpressionNode>(ExpressionType::Literal, foldAddition(left_child->value(),
      //   right_child->value()));
    }
  }
  return node;
}

}  // namespace opossum
