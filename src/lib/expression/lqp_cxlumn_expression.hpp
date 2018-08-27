#pragma once

#include "abstract_expression.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_cxlumn_reference.hpp"
#include "types.hpp"

namespace opossum {

class LQPCxlumnExpression : public AbstractExpression {
 public:
  explicit LQPCxlumnExpression(const LQPCxlumnReference& cxlumn_reference);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_cxlumn_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;
  bool requires_computation() const override;

  const LQPCxlumnReference cxlumn_reference;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
