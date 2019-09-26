#pragma once

#include "logical_query_plan/lqp_translator.hpp"

#if HYRISE_JIT_SUPPORT

#include "operators/jit_operator/operators/jit_expression.hpp"
#include "operators/jit_operator_wrapper.hpp"

namespace opossum {

/* This class can be used as a drop-in specialization for the LQPTranslator.
 * The JitAwareLQPTranslator will try to translate multiple AbstractLQPNodes into a single JitOperatorWrapper, whenever
 * that is possible and seems beneficial. Otherwise, it will fall back to the LQPTranslator.
 *
 * It works in two steps:
 * 1) Determine if we can/should add a JitOperatorWrapper node here and which nodes we can replace:
 *    Starting from the current node, we perform a breadth-first search through the query tree.
 *    For each node we will determine whether it is jittable (based on the node's type and parameters).
 *    We will follow each branch of the tree until we hit a non-jittable node. Since StoredTableNodes are not jittable,
 *    this is guaranteed to happen for all branches.
 *    All non-jittable nodes encountered this way are stored in a set.
 *    Once the breadth-first search terminates, we only continue if the number of jittable nodes is greater than two and
 *    the set of non-jittable nodes we encountered only contains a single node. This is then used as the input
 *    node to the chain of jit operators.
 * 2) Once we know which nodes we want to jit, we can start building out JitOperatorWrapper:
 *    We start by adding a JitReadTuples node. This node is passed to all translation functions during the construction
 *    of further operators. If any jit operator depends on a column or literal value, this value is registered with the
 *    JitReadTuples operator. The operator returns a JitTupleEntry that serves as a placeholder in the requesting
 *    operator. The JitReadTuples operator will make sure that the actual value is then accessible through the
 *    JitTupleEntry at runtime.
 *    The output columns are determined by the top-most ProjectionNode. If there is no ProjectionNode, all columns from
 *    the input node are considered as outputs.
 *    In case we find any PredicateNode or UnionNode during our traversal, we need to create a JitFilter operator.
 *    Whenever a non-primitive value (such as a predicate conditions, LQPExpression of LQPColumnReferences - which
 *    can in turn reference a LQPExpression in a ProjectionNode) is encountered, it is converted to an JitExpression
 *    by a helper method first. We then add a JitCompute operator to our chain and use its result value instead of the
 *    original non-primitive value.
 */
class JitAwareLQPTranslator final : public LQPTranslator {
 public:
  std::shared_ptr<AbstractOperator> translate_node(const std::shared_ptr<AbstractLQPNode>& node) const final;

 private:
  std::shared_ptr<JitOperatorWrapper> _try_translate_sub_plan_to_jit_operators(
      const std::shared_ptr<AbstractLQPNode>& node) const;

  /* Recursively translate an expression with its arguments to a JitExpression.
   * @param expression        The to be translated expression
   * @param jit_source        JitReadTuples operator used to add input columns, literals or parameters
   * @param input_node        Input node to check for input columns
   * @param use_actual_value  Specifies whether a column should either load an actual value or a value id
   * @return                  Translated expression
   */
  std::shared_ptr<JitExpression> _try_translate_expression_to_jit_expression(
      const std::shared_ptr<AbstractExpression>& expression, JitReadTuples& jit_source,
      const std::shared_ptr<AbstractLQPNode>& input_node, const bool use_actual_value = true) const;

  // Returns whether an LQP node with its current configuration can be part of an operator pipeline.
  bool _node_is_jittable(const std::shared_ptr<AbstractLQPNode>& node, const bool is_root_node) const;

  // Returns whether an expression can be part of a jittable operator pipeline.
  bool _expression_is_jittable(const std::shared_ptr<AbstractExpression>& expression) const;

  static JitExpressionType _expression_to_jit_expression_type(const std::shared_ptr<AbstractExpression>& expression);
};

}  // namespace opossum

#else

namespace opossum {

class JitAwareLQPTranslator final : public LQPTranslator {
 public:
  [[noreturn]] JitAwareLQPTranslator() {
    Fail("Query translation with JIT operators requested, but jitting is not available");
  }
};

}  // namespace opossum

#endif
