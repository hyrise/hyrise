#pragma once

#include "abstract_feature_node.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "operators/operator_join_predicate.hpp"

namespace hyrise {

class PredicateFeatureNode : public AbstractFeatureNode {
 public:
  PredicateFeatureNode(const std::shared_ptr<AbstractExpression>& lqp_expression,
                       const std::shared_ptr<AbstractExpression>& pqp_expression,
                       const std::shared_ptr<AbstractFeatureNode>& operator_node_input);

  PredicateFeatureNode(const std::shared_ptr<const BinaryPredicateExpression>& predicate_expression,
                       const OperatorJoinPredicate& join_predicate,
                       const std::shared_ptr<AbstractFeatureNode>& left_operator_node_input,
                       const std::shared_ptr<AbstractFeatureNode>& right_operator_node_input);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;

  std::optional<PredicateCondition> _predicate_condition;
  bool _column_vs_value = false;
  bool _column_vs_column = false;
  bool _is_complex = false;
};

}  // namespace hyrise
