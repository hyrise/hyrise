#include "abstract_feature_node.hpp"

#include "feature_extraction/util/feature_extraction_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractFeatureNode::AbstractFeatureNode(const FeatureNodeType type,
                                         const std::shared_ptr<AbstractFeatureNode>& left_input,
                                         const std::shared_ptr<AbstractFeatureNode>& right_input)
    : _type{type}, _left_input(left_input), _right_input(right_input) {}

std::shared_ptr<AbstractFeatureNode> AbstractFeatureNode::left_input() const {
  return _left_input;
}

std::shared_ptr<AbstractFeatureNode> AbstractFeatureNode::right_input() const {
  return _right_input;
}

FeatureNodeType AbstractFeatureNode::type() const {
  return _type;
}

const FeatureVector& AbstractFeatureNode::to_feature_vector() {
  // we gather features from static, executed PQPs. They do not change anymore, and we can cache the results
  if (_feature_vector) {
    return *_feature_vector;
  }

  _feature_vector = _on_to_feature_vector();
  Assert(_feature_vector->size() == feature_headers().size(), "Malformed feature vector");

  return *_feature_vector;
}

size_t AbstractFeatureNode::hash() const {
  size_t hash{0};

  visit_feature_nodes(shared_from_this(), [&hash](const auto& node) {
    if (node) {
      boost::hash_combine(hash, node->type());
      boost::hash_combine(hash, node->_on_shallow_hash());
      return FeatureNodeVisitation::VisitInputs;
    } else {
      return FeatureNodeVisitation::DoNotVisitInputs;
    }
  });

  return hash;
}

}  // namespace opossum
