#include "abstract_feature_node.hpp"

#include "utils/assert.hpp"

namespace opossum {

  AbstractFeatureNode::AbstractFeatureNode(const FeatureNodeType type,  const std::shared_ptr<Query>& query, const std::shared_ptr<AbstractFeatureNode>& left_input, const std::shared_ptr<AbstractFeatureNode>& right_input, const size_t run_time) : _type{type}, _query{query}, _left_input(left_input), _right_input(right_input), _run_time{run_time} {}

  size_t AbstractFeatureNode::run_time() const {
  	Assert(is_operator(), "Non-operator node does not have a run-time");
  	return _run_time;
  }

  std::shared_ptr<AbstractFeatureNode> AbstractFeatureNode::left_input() const {
  	return _left_input;

  }
  std::shared_ptr<AbstractFeatureNode> AbstractFeatureNode::right_input() const {
  	return _right_input;
  }

  std::shared_ptr<Query> AbstractFeatureNode::query() const {
  	return _query;
  }

  FeatureNodeType AbstractFeatureNode::type() const {
  	return _type;
  }

  bool AbstractFeatureNode::is_operator() const {
  	return _type == FeatureNodeType::Operator;
  }

}  // namespace opossum
