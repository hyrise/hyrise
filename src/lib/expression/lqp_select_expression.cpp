#include "lqp_select_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "utils/assert.hpp"


namespace opossum {

LQPSelectExpression::LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp,
                                         const std::vector<std::pair<ValuePlaceholder, std::shared_ptr<AbstractExpression>>>& referenced_external_expressions):
 lqp(lqp), referenced_external_expressions(referenced_external_expressions) {

}

std::shared_ptr<AbstractExpression> LQPSelectExpression::deep_copy() const {
  const auto lqp_copy = lqp->deep_copy();

  std::vector<std::pair<ValuePlaceholder, std::shared_ptr<AbstractExpression>>> copied_referenced_external_expressions;
  copied_referenced_external_expressions.reserve(referenced_external_expressions.size());

  for (const auto& referenced_external_expression : referenced_external_expressions) {
    copied_referenced_external_expressions.emplace_back(
      referenced_external_expression.first,
      referenced_external_expression.second->deep_copy()
    );
  }

  return std::make_shared<LQPSelectExpression>(lqp_copy, copied_referenced_external_expressions);
}

std::string LQPSelectExpression::as_column_name() const {
//  std::stringstream stream;
//  lqp->print(stream);
//  return stream.str();
  return "SUBSELECT";
}

DataType LQPSelectExpression::data_type() const {
  Assert(lqp->output_column_expressions().size() == 1, "Subselects must return one column");
  return lqp->output_column_expressions()[0]->data_type();
}

bool LQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  return !lqp_find_subplan_mismatch(lqp, static_cast<const LQPSelectExpression&>(expression).lqp);
}

size_t LQPSelectExpression::_on_hash() const {
  return 0;  // TODO(moritz)
}

}  // namespace opossum
