#include "predicate_feature_node.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "feature_extraction/feature_nodes/column_feature_node.hpp"
#include "feature_extraction/util/feature_extraction_utils.hpp"

namespace opossum {

PredicateFeatureNode::PredicateFeatureNode(const std::shared_ptr<AbstractExpression>& expression,
                                           const std::shared_ptr<AbstractFeatureNode>& operator_node)
    : AbstractFeatureNode(FeatureNodeType::Predicate, nullptr, nullptr) {
  Assert(expression->type == ExpressionType::Predicate, "Predicate expected");
  const auto predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(expression);
  _predicate_condition = predicate_expression->predicate_condition;
  if (const auto binary_predicate_expression =
          std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_expression)) {
    // TODO
  } else if (const auto is_null_expression = std::dynamic_pointer_cast<IsNullExpression>(predicate_expression)) {
    _column_vs_value = true;
    if (is_null_expression->operand()->type == ExpressionType::LQPColumn) {
      const auto column_id = ColumnID{0};  // TO DO
      _left_input = ColumnFeatureNode::from_expression(operator_node, is_null_expression->operand(), column_id);
    }
  } else if (const auto between_expression = std::dynamic_pointer_cast<BetweenExpression>(predicate_expression)) {
    // TODO
  } else {
    _is_complex = true;
  }
}

std::shared_ptr<FeatureVector> PredicateFeatureNode::_on_to_feature_vector() const {
  // auto feature_vector = one_hot_encoding<PredicateType>(_predicate_type);
  // auto condition_vector = one_hot_encoding<PredicateCondition>(_predicate_condition);
  // feature_vector->insert(feature_vector->end(), condition_vector->cbegin(), condition_vector->cend());
  // return feature_vector;
  return nullptr;  // TODO
}

const std::vector<std::string>& PredicateFeatureNode::feature_headers() const {
  return headers();
}

const std::vector<std::string>& PredicateFeatureNode::headers() {
  // static auto ohe_headers_predicate = one_hot_headers<PredicateType>("predicate.");
  // static const auto ohe_headers_condition = one_hot_headers<PredicateCondition>("condition.");
  // if (ohe_headers_type.size() == magic_enum::enum_count<PredicateType>()) {
  //   ohe_headers_type.insert(ohe_headers_type.end(), ohe_headers_condition.begin(), ohe_headers_condition.end());
  // }
  // return ohe_headers_type;
  static auto res = std::vector<std::string>{};
  return res;  // TO DO
}

}  // namespace opossum
