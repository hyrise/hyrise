#pragma once

#include <utility>

#include "abstract_expression.hpp"
#include "parameter_expression.hpp"

namespace opossum {

class AbstractLQPNode;

class LQPSelectExpression : public AbstractExpression {
 public:
  LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<ParameterID>& parameter_ids,
                      const std::vector<std::shared_ptr<AbstractExpression>>& parameter_expressions);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  size_t parameter_count() const;
  std::shared_ptr<AbstractExpression> parameter_expression(const size_t parameter_idx) const;

  const std::shared_ptr<AbstractLQPNode> lqp;
  const std::vector<ParameterID> parameter_ids;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
