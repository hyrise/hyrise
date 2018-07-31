#pragma once

#include <utility>

#include "abstract_expression.hpp"
#include "parameter_expression.hpp"

namespace opossum {

class AbstractLQPNode;

/**
 * Parameters are passed in as two vectors `parameter_ids` and `parameter_expressions` that need to have the same
 * length.
 * Each parameter_expression is assigned the ParameterID at the same index in parameter_ids.
 * (Two separate vectors are used instead of a vector of pairs so `parameter_expressions` can be passed to the
 * AbstractExpression as they are the `arguments` to the LQPSelectExpression)
 *
 * Within the wrapped LQP, the parameter_expressions will be referenced using these ParameterIDs
 * This avoids pointers from the wrapped LQP into the outer LQP (which would be a nightmare to maintain in deep_copy())
 */
class LQPSelectExpression : public AbstractExpression {
 public:
  LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<ParameterID>& parameter_ids,
                      const std::vector<std::shared_ptr<AbstractExpression>>& parameter_expressions);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  size_t parameter_count() const;
  std::shared_ptr<AbstractExpression> parameter_expression(const size_t parameter_idx) const;

  std::shared_ptr<AbstractLQPNode> lqp;
  const std::vector<ParameterID> parameter_ids;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
