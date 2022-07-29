#include "operator_feature_node.hpp"

#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace {

using namespace opossum;

std::shared_ptr<OperatorFeatureNode> _recursively_build_graph_from_pqp(
    const std::shared_ptr<const AbstractOperator>& op) {
  const auto left_input = op->left_input() ? _recursively_build_graph_from_pqp(op->left_input()) : nullptr;
  const auto right_input = op->right_input() ? _recursively_build_graph_from_pqp(op->right_input()) : nullptr;

  return std::make_shared<OperatorFeatureNode>(op, left_input, right_input);
}

}  // namespace

namespace opossum {

OperatorFeatureNode::OperatorFeatureNode(const std::shared_ptr<const AbstractOperator>& op,
                                         const std::shared_ptr<AbstractFeatureNode>& left_input,
                                         const std::shared_ptr<AbstractFeatureNode>& right_input)
    : AbstractFeatureNode{FeatureNodeType::Operator, left_input, right_input},
      _op{op},
      _op_type{op->type()},
      _run_time{op->performance_data->walltime},
      _output_table{ResultTableFeatureNode::from_operator(op)} {}

std::shared_ptr<OperatorFeatureNode> OperatorFeatureNode::from_pqp(const std::shared_ptr<const AbstractOperator>& op,
                                                                   const std::shared_ptr<Query>& query) {
  auto root_node = _recursively_build_graph_from_pqp(op);
  root_node->set_as_root_node(query);
  return root_node;
}

size_t OperatorFeatureNode::_on_shallow_hash() const {
  const auto& lqp_node = _op->lqp_node;
  Assert(lqp_node, "Operator does not have LQPNode");
  return lqp_node->hash();
  /*size_t hash{0};

  for (const auto& predicate : _predicates) {
    boost::hash_combine(hash, predicate->type());
    boost::hash_combine(hash, predicate->hash());
  }

  boost::hash_combine(hash, _op_type);

  return hash;*/
}

std::shared_ptr<FeatureVector> OperatorFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<OperatorType>(_op_type);
  const auto& output_feature_vector = _output_table->to_feature_vector();
  feature_vector->insert(feature_vector->end(), output_feature_vector.begin(), output_feature_vector.end());
  return feature_vector;
}

const std::vector<std::string>& OperatorFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& OperatorFeatureNode::headers() {
  static auto ohe_headers_type = one_hot_headers<OperatorType>("operator_type.");
  static const auto output_headers = AbstractTableFeatureNode::headers();
  if (ohe_headers_type.size() == magic_enum::enum_count<OperatorType>()) {
    ohe_headers_type.insert(ohe_headers_type.end(), output_headers.begin(), output_headers.end());
  }
  return ohe_headers_type;
}

std::chrono::nanoseconds OperatorFeatureNode::run_time() const {
  return _run_time;
}

bool OperatorFeatureNode::is_root_node() const {
  return _is_root_node;
}

void OperatorFeatureNode::set_as_root_node(const std::shared_ptr<Query>& query) {
  Assert(!_is_root_node, "Root Node Property should only be set once");
  _is_root_node = true;
  _query = query;
}

std::shared_ptr<Query> OperatorFeatureNode::query() const {
  Assert(_is_root_node, "Only root node is assigned to query");
  return _query;
}

std::shared_ptr<const AbstractOperator> OperatorFeatureNode::get_operator() const {
  return _op;
}

std::shared_ptr<ResultTableFeatureNode> OperatorFeatureNode::output_table() const {
  return _output_table;
}

const std::vector<std::shared_ptr<AbstractFeatureNode>>& OperatorFeatureNode::subqueries() const {
  return _subqueries;
}

}  // namespace opossum
