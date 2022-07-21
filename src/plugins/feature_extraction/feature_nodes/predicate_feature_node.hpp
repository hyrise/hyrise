#pragma once

#include "abstract_feature_node.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class PredicateFeatureNode : public AbstractFeatureNode {
 public:
  PredicateFeatureNode(const std::shared_ptr<AbstractExpression>& expression,
                       const std::shared_ptr<AbstractFeatureNode>& operator_node);

  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() const final;
  size_t _on_shallow_hash() const final;

  PredicateCondition _predicate_condition;
  bool _column_vs_value = false;
  bool _column_vs_column = false;
};

}  // namespace opossum
