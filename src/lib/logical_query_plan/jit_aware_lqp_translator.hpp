#pragma once

#include "lqp_translator.hpp"
#include "operators/jit_operator.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"

namespace opossum {

class JitAwareLQPTranslator final : protected LQPTranslator {
 public:
  std::shared_ptr<AbstractOperator> translate_node(const std::shared_ptr<AbstractLQPNode>& node) const final;

 private:
  JitExpression::Ptr _translate_to_jit_expression(const std::shared_ptr<AbstractLQPNode>& node,
                                                  JitReadTable& jit_source) const;
  JitExpression::Ptr _translate_to_jit_expression(const std::shared_ptr<PredicateNode>& node,
                                                  JitReadTable& jit_source) const;
  JitExpression::Ptr _translate_to_jit_expression(const LQPExpression& lqp_expression, JitReadTable& jit_source) const;
  JitExpression::Ptr _translate_to_jit_expression(const LQPColumnReference& lqp_column_reference,
                                                  JitReadTable& jit_source) const;
  JitExpression::Ptr _translate_to_jit_expression(const AllParameterVariant& value, JitReadTable& jit_source) const;

  bool _has_another_condition(const std::shared_ptr<AbstractLQPNode>& node) const;

  void _breadth_first_search(const std::shared_ptr<AbstractLQPNode>& node,
                             std::function<bool(const std::shared_ptr<AbstractLQPNode>&)> func) const;
};

}  // namespace opossum
