#pragma once

#include "abstract_feature_node.hpp"
#include "expression/aggregate_expression.hpp"

namespace hyrise {

class AggregateFunctionFeatureNode : public AbstractFeatureNode {
 public:
  AggregateFunctionFeatureNode(const std::shared_ptr<AbstractExpression>& lqp_expression,
                               const std::shared_ptr<AbstractExpression>& pqp_expression,
                               const std::shared_ptr<AbstractFeatureNode>& operator_node_input);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;

  AggregateFunction _aggregate_function{AggregateFunction::Any};
  bool _is_count_star{false};
};

}  // namespace hyrise
