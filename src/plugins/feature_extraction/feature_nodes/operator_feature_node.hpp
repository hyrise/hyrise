#pragma once

#include "abstract_feature_node.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

class OperatorFeatureNode : public AbstractFeatureNode {
 public:

  OperatorFeatureNode(const std::shared_ptr<const AbstractOperator>& op, const std::shared_ptr<AbstractFeatureNode>& left_input, const std::shared_ptr<AbstractFeatureNode>& right_input = nullptr, const std::shared_ptr<Query>& query = nullptr);

  size_t hash() const final;


  const std::vector<std::string>& feature_headers() const final;

  static const std::vector<std::string>& headers();

 protected:
  std::shared_ptr<FeatureVector> _on_to_feature_vector() final;

  std::vector<std::shared_ptr<AbstractFeatureNode>> _predicates;
  std::weak_ptr<AbstractFeatureNode> _output_table;
  std::shared_ptr<const AbstractOperator> _op;
  OperatorType _op_type;
};

}  // namespace opossum
