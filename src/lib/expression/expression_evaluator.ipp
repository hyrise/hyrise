//#include "expression_evaluator.hpp"
//
//#include "abstract_expression.hpp"
//#include "arithmetic_expression.hpp"
//#include "pqp_column_expression.hpp"
//#include "pqp_column_expression.hpp"
//
//namespace opossum {
//
//ExpressionEvaluator::ExpressionEvaluator(const std::shared_ptr<Chunk>& chunk):
//  _chunk(chunk)
//{}
//
//template<typename T>
//ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_expression(const AbstractExpression& expression) const {
//  switch (expression.type) {
//    case ExpressionType::Arithmetic:
//      return evaluate_arithmetic_expression<T>(static_cast<const ArithmeticExpression&>(expression));
//
//    case ExpressionType::Column: {
////      const auto* pqp_column_expression = dynamic_cast<const PQPColumnExpression*>(&expression);
////      Assert(pqp_column_expression, "Can only evaluate PQPColumnExpressions");
//
//      //return evaluate_column<T>(*pqp_column_expression);
//    }
//
//    default:
//      Fail("ExpressionType evaluation not yet implemented");
//  }
//}
//
//template<typename T>
//ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_arithmetic_expression(const ArithmeticExpression& expression) const {
//  const auto left_operands = evaluate_expression<T>(*expression.left_operand());
//  const auto right_operands = evaluate_expression<T>(*expression.right_operand());
//
//  switch (expression.arithmetic_operator) {
//    case ArithmeticOperator::Addition:
//      return evaluate_binary_operator(left_operands, right_operands, std::plus<T>{});
//
//
//    default:
//      Fail("ArithmeticOperator evaluation not yet implemented");
//  }
//}
//
//template<typename T, typename OperatorFunctor>
//ExpressionEvaluator::ExpressionResult<T> ExpressionEvaluator::evaluate_binary_operator(const ExpressionResult<T>& left_operand,
//                                                             const ExpressionResult<T>& right_operand,
//                                                             const OperatorFunctor& functor) {
//
//  if (left_operand.type() == typeid(NullableValues<T>) && right_operand.type() == typeid(NullableValues<T>)) {
//    std::vector<bool> nulls(boost::get<NullableValues<T>>(left_operand).first.size());
//    std::transform()
//  }
//
//  ExpressionResult<T> result(left_operand.size());
//  std::transform(left_operand.begin(), left_operand.end(), right_operand.begin(), result.begin(), functor);
//  return result;
//}
//
//}  // namespace opossum
