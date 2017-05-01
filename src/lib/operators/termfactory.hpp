#pragma once
#include <map>
#include <memory>
#include <string>
#include "term.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
namespace opossum {
/**
 * The termfactory transforms complex string expressions into separate terms of specific types.
 *
 * "$a+2" will be converted to the term combination
 * ArithmeticTerm(VariableTerm("a"), ConstantTerm(2), "+")
 *
 * These can be further processed by opossum. See terms.hpp for details.
 *
 */
template <typename T>
class TermFactory {
 protected:
  TermFactory() = default;
  static std::shared_ptr<AbstractTerm<T>> _build_binary_term(const std::string& expression) {
    DebugAssert(!(expression.empty()), "Empty expression.");

    if (expression.front() == table_name_escape) {
      return std::make_shared<VariableTerm<T>>(ColumnName(expression.substr(1)));
    } else {
      T value = type_cast<T>(expression);
      return std::make_shared<ConstantTerm<T>>(value);
    }
  }
  static std::shared_ptr<AbstractTerm<T>> _build_binary_term(const std::string& op, const std::string& left,
                                                             const std::string& right) {
    // check left side for further operators
    auto next_left_op_pos = left.find_first_of("+-");
    if (next_left_op_pos == std::string::npos) {
      next_left_op_pos = left.find_first_of("*/%");
    }
    // check right side for further operators
    auto next_right_op_pos = right.find_first_of("+-");
    if (next_right_op_pos == std::string::npos) {
      next_right_op_pos = right.find_first_of("*/%");
    }
    if (next_left_op_pos == std::string::npos && next_right_op_pos == std::string::npos) {
      return std::make_shared<ArithmeticTerm<T>>(_build_binary_term(left), _build_binary_term(right), op);
    }
    if (next_left_op_pos != std::string::npos) {
      return _build_binary_term(op, left, _build_binary_term(right));
    }
    if (next_right_op_pos != std::string::npos) {
      return _build_binary_term(op, _build_binary_term(left), right);
    }
    return std::make_shared<ArithmeticTerm<T>>(
        _build_binary_term(left.substr(next_left_op_pos, 1), left.substr(0, next_left_op_pos),
                           left.substr(next_left_op_pos)),
        _build_binary_term(right.substr(next_right_op_pos, 1), right.substr(0, next_right_op_pos),
                           right.substr(next_right_op_pos)),
        op);
  }
  static std::shared_ptr<AbstractTerm<T>> _build_binary_term(const std::string& op,
                                                             const std::shared_ptr<AbstractTerm<T>>& left,
                                                             const std::string& right) {
    auto next_op_pos = right.find_first_of("+-");
    if (next_op_pos == std::string::npos) {
      next_op_pos = right.find_first_of("*/%");
    }
    return std::make_shared<ArithmeticTerm<T>>(
        left,
        _build_binary_term(right.substr(next_op_pos, 1), right.substr(0, next_op_pos), right.substr(next_op_pos + 1)),
        op);
  }
  static std::shared_ptr<AbstractTerm<T>> _build_binary_term(const std::string& op, const std::string& left,
                                                             const std::shared_ptr<AbstractTerm<T>>& right) {
    auto next_op_pos = left.find_first_of("+-");
    if (next_op_pos == std::string::npos) {
      next_op_pos = left.find_first_of("*/%");
    }
    return std::make_shared<ArithmeticTerm<T>>(
        _build_binary_term(left.substr(next_op_pos, 1), left.substr(0, next_op_pos), left.substr(next_op_pos + 1)),
        right, op);
  }

 public:
  static std::shared_ptr<AbstractTerm<T>> build_term(const std::string expression) {
    // we will solve the expression recursively, so find +- first
    auto op_pos = expression.find_first_of("+-");
    if (op_pos == std::string::npos) {
      op_pos = expression.find_first_of("*/%");
    }
    if (op_pos != std::string::npos) {
      // the expression is complex and contains at least one operator
      return _build_binary_term(expression.substr(op_pos, 1), expression.substr(0, op_pos),
                                expression.substr(op_pos + 1));
    } else {
      // the expression only contains a column name or a constant
      return _build_binary_term(expression);
    }
  }
  static const char table_name_escape = '$';
};
}  // namespace opossum
