#pragma once

#include "lqp_translator.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator_wrapper.hpp"

namespace opossum {

/* This class can be used as a drop-in replacement for the LQPTranslator.
 * The JitAwareLQPTranslator will try to translate multiple AbstractLQPNodes into a single JitOperatorWrapper, whenever
 * that is possible and seems beneficial. Otherwise it will fall back to the LQPTranslator.
 *
 * It works in two steps:
 * 1) Determine, if we can/should add a JitOperatorWrapper node here and which nodes we can replace:
 *    Starting from the current node, we perform a BFS through the query tree. For each node we will determine, whether
 *    it is jitable (based on the node's type and parameters). We will follow each branch of the tree until we hit a
 *    non-jitable node. Since StoredTableNodes are not jitable, this is guaranteed to happen for all branches.
 *    All non-jitable nodes encountered this way are stored in a set.
 *    Once the BFS terminates, we only continue, if the number of jitable nodes is above some threshold and the
 *    set of non-jitable nodes we encountered only contains a single node. This is then used as the input
 *    node to the chain of jit operators.
 * 2) Once we know which nodes we want to jit, we can start building out JitOperatorWrapper:
 *    We start by adding a JitReadTuple node. This node is passed to all translation functions during the construction
 *    of further operators. If any jit operator depends on a column or literal value, this value is registered with the
 *    JitReadTuple operator. The operator returns a JitTupleValue that serves as a placeholder in the requesting
 *    operator. The JitReadTuple operator will make sure, that the actual value is then accessible through the
 *    JitTupleValue at runtime.
 *    The output columns are determined by the top-most ProjectionNode. If there is no ProjectionNode, all columns from
 *    the input node are considered as outputs.
 *    In case we find any PredicateNode or UnionNode during our traversal, we need to a JitFilter operator.
 *    Whenever a non-primitive value (such as a predicate conditions, LQPExpression of LQPColumnReferences - which
 *    can in turn reference a LQPExpression in a ProjectionNode) is encountered, it is converted to an JitExpression
 *    by a helper method first. We then add a JitCompute operator to our chain and use its result value instead of the
 *    original non-primitive value.
 */
class JitAwareLQPTranslator final : protected LQPTranslator {
 public:
  std::shared_ptr<AbstractOperator> translate_node(const std::shared_ptr<AbstractLQPNode>& node) const final;

 private:
  std::shared_ptr<const JitExpression> _translate_to_jit_expression(
      const std::shared_ptr<AbstractLQPNode>& node, JitReadTuple& jit_source,
      const std::shared_ptr<AbstractLQPNode>& input_node) const;
  std::shared_ptr<const JitExpression> _translate_to_jit_expression(
      const std::shared_ptr<PredicateNode>& node, JitReadTuple& jit_source,
      const std::shared_ptr<AbstractLQPNode>& input_node) const;
  std::shared_ptr<const JitExpression> _translate_to_jit_expression(
      const LQPExpression& lqp_expression, JitReadTuple& jit_source,
      const std::shared_ptr<AbstractLQPNode>& input_node) const;
  std::shared_ptr<const JitExpression> _translate_to_jit_expression(
      const LQPColumnReference& lqp_column_reference, JitReadTuple& jit_source,
      const std::shared_ptr<AbstractLQPNode>& input_node) const;
  std::shared_ptr<const JitExpression> _translate_to_jit_expression(
      const AllParameterVariant& value, JitReadTuple& jit_source,
      const std::shared_ptr<AbstractLQPNode>& input_node) const;

  bool _has_another_condition(const std::shared_ptr<AbstractLQPNode>& node) const;

  bool _node_is_jitable(const std::shared_ptr<AbstractLQPNode>& node) const;

  void _breadth_first_search(const std::shared_ptr<AbstractLQPNode>& node,
                             std::function<bool(const std::shared_ptr<AbstractLQPNode>&)> func) const;
};

}  // namespace opossum
