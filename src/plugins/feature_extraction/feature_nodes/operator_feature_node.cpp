#include "operator_feature_node.hpp"

#include "feature_extraction/feature_nodes/table_feature_node.hpp"
#include "feature_extraction/util/one_hot_encoding.hpp"

namespace opossum {

OperatorFeatureNode::OperatorFeatureNode(const std::shared_ptr<const AbstractOperator>& op, const std::shared_ptr<AbstractFeatureNode>& left_input, const std::shared_ptr<AbstractFeatureNode>& right_input, const std::shared_ptr<Query>& query) : AbstractFeatureNode{FeatureNodeType::Operator, left_input, right_input, op->performance_data->walltime, query}, _op{op}, _op_type{op->type()} {}

size_t OperatorFeatureNode::hash() const {
  const auto& lqp_node = _op->lqp_node;
  Assert(lqp_node, "Operator does not have LQPNode");
  return lqp_node->hash();
}

std::shared_ptr<FeatureVector>  OperatorFeatureNode::_on_to_feature_vector() {
  if (_output_table.expired()) {
    _output_table = TableFeatureNode::from_performance_data(*_op->performance_data, shared_from_this());
  }

  auto feature_vector = one_hot_encoding<OperatorType>(_op_type);
  const auto& output_feature_vector = _output_table.lock()->to_feature_vector();
  feature_vector->insert(feature_vector->end(), output_feature_vector.begin(), output_feature_vector.end());
  return feature_vector;
}

const std::vector<std::string>& OperatorFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& OperatorFeatureNode::headers() {
  static auto ohe_headers_type = one_hot_headers<OperatorType>("operator_type.");
  static const auto output_headers = TableFeatureNode::headers();
  if (ohe_headers_type.size() == magic_enum::enum_count<OperatorType>()) {
    ohe_headers_type.insert(ohe_headers_type.end(), output_headers.begin(), output_headers.end());
  }
  return ohe_headers_type;
}


}  // namespace opossum
