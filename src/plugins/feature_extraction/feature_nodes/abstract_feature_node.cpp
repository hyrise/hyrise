#include "abstract_feature_node.hpp"

#include "utils/assert.hpp"

namespace opossum {

AbstractFeatureNode::AbstractFeatureNode(const FeatureNodeType type, const std::shared_ptr<AbstractFeatureNode>& left_input, const std::shared_ptr<AbstractFeatureNode>& right_input, const std::chrono::nanoseconds& run_time, const std::shared_ptr<Query>& query) : _type{type}, _left_input(left_input), _right_input(right_input), _run_time{run_time}, _query{query} {}

std::chrono::nanoseconds AbstractFeatureNode::run_time() const {
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
	Assert(is_root_node(), "Only root node is assigned to query");
	return _query;
}

FeatureNodeType AbstractFeatureNode::type() const {
	return _type;
}

bool AbstractFeatureNode::is_operator() const {
	return _type == FeatureNodeType::Operator;
}

bool AbstractFeatureNode::is_root_node() const {
	Assert(is_operator(), "Only operator can be root node");
	return _is_root_node;
}

void AbstractFeatureNode::set_as_root_node() {
  Assert(!_is_root_node, "Root Node Property should only be set once");
  _is_root_node = true;
}

const FeatureVector& AbstractFeatureNode::to_feature_vector() {
	if (_feature_vector) {
		return *_feature_vector;
	}

	_feature_vector = _on_to_feature_vector();
	Assert(_feature_vector->size() == feature_headers().size(), "Malformed feature vector");

	return *_feature_vector;
}

}  // namespace opossum
