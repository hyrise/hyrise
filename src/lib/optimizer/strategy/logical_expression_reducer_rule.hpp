#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * TODO
 */
class LogicalExpressionReducerRule : public AbstractRule {
 public:
  std::string name() const override;
  bool apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

 private:
  using MapType = ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>;
  bool _apply_to_node(const std::shared_ptr<AbstractLQPNode>& node, MapType& previously_reduced_expressions) const;
  bool _apply_to_expressions(std::vector<std::shared_ptr<AbstractExpression>>& expressions,
                             MapType& previously_reduced_expressions) const;
};

}  // namespace opossum
