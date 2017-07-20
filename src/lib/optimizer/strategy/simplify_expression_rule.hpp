#pragma once

#include <memory>

#include "abstract_rule.hpp"
#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/expression/expression_node.hpp"

namespace opossum {
class SimplifyExpressionRule : public AbstractRule {
 public:
  std::shared_ptr<AbstractASTNode> apply_rule(std::shared_ptr<AbstractASTNode> node) override;

 private:
  std::shared_ptr<ExpressionNode> simplify_addition_of_literals(std::shared_ptr<ExpressionNode> node);

  //  template <typename LeftType, typename  RightType>
  //  AllTypeVariant foldAddition(LeftType a, RightType b);
};

}  // namespace opossum
