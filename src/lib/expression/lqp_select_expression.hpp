#pragma once

#include <utility>

#include "abstract_select_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class ExternalExpression;

class LQPSelectExpression : public AbstractSelectExpression {
 public:
  LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp,
                      const std::vector<std::pair<ValuePlaceholder, std::shared_ptr<AbstractExpression>>>& referenced_external_expressions);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  const std::shared_ptr<AbstractLQPNode> lqp;
  const std::vector<std::pair<ValuePlaceholder, std::shared_ptr<AbstractExpression>>> referenced_external_expressions;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
