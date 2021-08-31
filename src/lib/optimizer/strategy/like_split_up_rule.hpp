#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/logical_expression.hpp"

namespace opossum {

class AbstractLQPNode;

class LikeSplitUpRule : public AbstractRule {
 public:
  std::string name() const override;

  static void split_up_like(std::shared_ptr<AbstractLQPNode> sub_node,
                            std::shared_ptr<AbstractExpression>& input_expression);

 protected:
  void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const override;
};

}  // namespace opossum
