#include "predicate_feature_node.hpp"

#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace opossum {

PredicateFeatureNode::PredicateFeatureNode(const std::shared_ptr<AbstractExpression>& expression,
                                           const std::shared_ptr<AbstractFeatureNode>& operator_node)
    : AbstractFeatureNode(nullptr, nullptr) {
  Assert(expression->type == ExpressionType::Predicate);
  const auto predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(expression);
  _predicate_condition = predicate_expression->predicate_condition;
  if (const auto binary_predicate_expression =
          std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_expression)) {
  } else if (const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(predicate_expression)) {
    _column_vs_value = true;
    if (is_null_expression->operand->type == LQPNodeType::LQPColumn) {
      _left_input = ColumnFeatureNode::from_column_expression(is_null_expression->operand, operator_node);
    }
  } else if (const auto between_expression = std::dynamic_pointer_cast<BetweenExpression>(predicate_expression)) {
  } else {
    _is_complex = true;
  }
}

size_t PredicateFeatureNode::_on_shallow_hash() const {
  auto hash = boost::hash_value<> boost::hash_combine(hash, _table_type);
  return hash;
}

std::shared_ptr<FeatureVector> PredicateFeatureNode::_on_to_feature_vector() const {
  auto feature_vector = one_hot_encoding<PredicateType>(_predicate_type);
  auto condition_vector = one_hot_encoding<PredicateCondition>(_predicate_condition);
  feature_vector->insert(feature_vector->end(), condition_vector->cbegin(), condition_vector->cend());
  return feature_vector;
}

const std::vector<std::string>& PredicateFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& PredicateFeatureNode::headers() {
  static auto ohe_headers_predicate = one_hot_headers<PredicateType>("predicate.");
  static const auto ohe_headers_condition = one_hot_headers<PredicateCondition>("condition.");
  if (ohe_headers_type.size() == magic_enum::enum_count<PredicateType>()) {
    ohe_headers_type.insert(ohe_headers_type.end(), ohe_headers_condition.begin(), ohe_headers_condition.end());
  }
  return ohe_headers_type;
}

}  // namespace opossum
