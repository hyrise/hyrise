#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_rule.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;

/**
 * This optimizer rule is responsible for inserting a limit node in the correlated subquery of an exists expression.
 * The query         SELECT * FROM T WHERE EXISTS ( SELECT * FROM T WHERE C = 1 );
 * is transformed to SELECT * FROM T WHERE EXISTS ( SELECT * FROM T WHERE C = 1 LIMIT 1 );
 * Jittable operators profit from the additional limit as this allows them to terminate early during processing.
 */
class InsertLimitInExistsRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
