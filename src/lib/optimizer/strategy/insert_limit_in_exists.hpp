#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;

// This optimizer rule is responsible for inserting a limit node in the correlated subquery of an exists expression
class InsertLimitInExistsRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;

private:
  void _apply_to_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions) const;
};

}  // namespace opossum
