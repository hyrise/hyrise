#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "storage/dictionary_column.hpp"
#include "storage/reference_column.hpp"
#include "storage/value_column.hpp"

#include "types.hpp"

namespace opossum {

class Table;

/**
 * Terms represent either a constant value or a column name.
 * They should be used to generalize internal Opossum functions.
 *
 * Use term compositions to create (arithmetic) expressions.
 *
 * There are two atomic term types:
 *   1. ConstantTerm (AllTypeVariant)
 *   2. VariableTerm (ColumnName)
 *
 * Any two of the above can be combined in an:
 *   3. ArithmeticTerm (Term, Term, OperatorString)
 *
 * As an extra, arithmetic terms can be part of another arithmetic term.
 * They will be evaluated recursively and therefore allow complex expressions.
 *
 *                            +--------------+
 *                            | AbstractTerm |
 *                            +--------------+
 *                                   |
 *               +-------------------+----------------------+
 *               |                   |                      |
 *   +------------------+    +----------------+    +------------------+
 *   |   ConstantTerm   |    |  VariableTerm  |    |  ArithmeticTerm  |
 *   | (AllTypeVariant) |    |  (ColumnName)  |    |   Composition    |
 *   +------------------+    +----------------+    +------------------+
 *
 */
template <typename T>
class AbstractTerm {
 public:
  virtual ~AbstractTerm() = default;
  virtual const tbb::concurrent_vector<T> get_values(const std::shared_ptr<const Table> table,
                                                     const ChunkID chunk_id) = 0;
};

/**
 * A constant term stores an AllTypeVariant.
 * It doesn't change after creation.
 */
template <typename T>
class ConstantTerm : public AbstractTerm<T> {
 public:
  explicit ConstantTerm(const AllTypeVariant& value) : _value(type_cast<T>(value)) {}

  const tbb::concurrent_vector<T> get_values(const std::shared_ptr<const Table> table, const ChunkID chunk_id) final {
    return tbb::concurrent_vector<T>(table->get_chunk(chunk_id).size(), _value);
  }

  const T get_value() { return _value; }

 protected:
  const T _value;
};

/**
 * A variable term stores just a column name.
 * It's return value is evaluated on demand and depends on the given table position.
 */
template <typename T>
class VariableTerm : public AbstractTerm<T> {
 public:
  explicit VariableTerm(const ColumnName& column_name) : _column_name(column_name) {}

  // for a simple projection (SELECT a FROM) we just bypass a column - no need to materialize (also for reference and
  // dict)
  std::shared_ptr<BaseColumn> get_column(const std::shared_ptr<const Table> table, const ChunkID chunk_id) {
    return table->get_chunk(chunk_id).get_column(table->column_id_by_name(_column_name));
  }

  const tbb::concurrent_vector<T> get_values(const std::shared_ptr<const Table> table, const ChunkID chunk_id) {
    auto column = table->get_chunk(chunk_id).get_column(table->column_id_by_name(_column_name));

    if (auto value_column = std::dynamic_pointer_cast<ValueColumn<T>>(column)) {
      // values are copied
      return value_column->values();
    }
    if (auto dict_column = std::dynamic_pointer_cast<DictionaryColumn<T>>(column)) {
      return dict_column->materialize_values();
    }
    if (auto ref_column = std::dynamic_pointer_cast<ReferenceColumn>(column)) {
      return ref_column->template materialize_values<T>();  // Clang needs the template prefix
    }

    throw std::runtime_error("Materializing chunk failed.");
  }

 protected:
  const ColumnName _column_name;
};

/**
 * An arithmetic term stores two terms of any kind as well as an operator string, see get_operator().
 * The return value will be calculated recursively, solving the given terms.
 */
template <typename T>
class ArithmeticTerm : public AbstractTerm<T> {
 public:
  ArithmeticTerm(const std::shared_ptr<AbstractTerm<T>>& left_term, const std::shared_ptr<AbstractTerm<T>>& right_term,
                 const std::string& string_op)
      : _left(left_term), _right(right_term), _operator(get_operator(string_op)) {}

  // recursive solving of terms
  const tbb::concurrent_vector<T> get_values(const std::shared_ptr<const Table> table, const ChunkID chunk_id) final {
    tbb::concurrent_vector<T> values;
    values.resize(table->get_chunk(chunk_id).size());

    // optimization for Constant Terms: the value for a constant term can be directly binded to the operator function
    auto constant_term_right = std::dynamic_pointer_cast<ConstantTerm<T>>(_right);
    auto constant_term_left = std::dynamic_pointer_cast<ConstantTerm<T>>(_left);
    if (constant_term_right && constant_term_left) {
      std::fill(values.begin(), values.end(),
                _operator(constant_term_left->get_value(), constant_term_right->get_value()));
    } else if (constant_term_right) {
      auto left_values = _left->get_values(table, chunk_id);
      T right_value = constant_term_right->get_value();
      // apply operator function to both vectors
      std::transform(left_values.begin(), left_values.end(), values.begin(),
                     [this, right_value](T left_value) { return _operator(left_value, right_value); });

    } else if (constant_term_left) {
      T left_value = constant_term_left->get_value();
      auto right_values = _right->get_values(table, chunk_id);
      // apply operator function to both vectors
      std::transform(right_values.begin(), right_values.end(), values.begin(),
                     [this, left_value](T right_value) { return _operator(left_value, right_value); });

    } else {
      auto left_values = _left->get_values(table, chunk_id);
      auto right_values = _right->get_values(table, chunk_id);
      // apply operator function to both vectors
      std::transform(left_values.begin(), left_values.end(), right_values.begin(), values.begin(), _operator);
    }

    return values;
  }

 protected:
  /**
   * Generic arithmetic operator implementation for all (numeric) types.
   *
   * @param string_op is one of "+", "-", "*", "/", "%"
   * @returns a lambda function to solve arithmetic terms
   *
   */
  std::function<T(const T&, const T&)> get_operator(const std::string& string_op) {
    if (string_op == "+") {
      return std::plus<T>();
    } else if (string_op == "-") {
      return std::minus<T>();
    } else if (string_op == "*") {
      return std::multiplies<T>();
    } else if (string_op == "/") {
      return std::divides<T>();
    } else if (string_op == "%") {
      return std::modulus<T>();
    } else {
      throw std::runtime_error("Unknown arithmetic operator" + string_op);
    }
  }

  const std::shared_ptr<AbstractTerm<T>> _left;
  const std::shared_ptr<AbstractTerm<T>> _right;
  std::function<const T(const T&, const T&)> _operator;
};

/**
 * Specialized arithmetic operator implementation for std::string.
 * Two string terms can be added. Anything else is undefined.
 *
 * @param string_op has to be "+" for now
 * @returns a lambda function to solve arithmetic string terms
 *
 */
template <>
inline std::function<std::string(const std::string&, const std::string&)> ArithmeticTerm<std::string>::get_operator(
    const std::string& string_op) {
  if (string_op == "+") {
    return std::plus<std::string>();
  }
  throw std::runtime_error("Arithmetic operator " + string_op + " not defined for std::string");
}
/**
 * Specialized arithmetic operator implementation for float.
 * Modulo on float isn't defined.
 *
 * @param "+", "-", "*" or "/"
 * @returns a lambda function to solve arithmetic float terms
 *
 */
template <>
inline std::function<float(const float&, const float&)> ArithmeticTerm<float>::get_operator(
    const std::string& string_op) {
  if (string_op == "+") {
    return std::plus<float>();
  } else if (string_op == "-") {
    return std::minus<float>();
  } else if (string_op == "*") {
    return std::multiplies<float>();
  } else if (string_op == "/") {
    return std::divides<float>();
  } else {
    throw std::runtime_error("Unknown arithmetic operator" + string_op);
  }
}
/**
 * Specialized arithmetic operator implementation for double.
 * Modulo on double isn't defined.
 *
 * @param "+", "-", "*" or "/"
 * @returns a lambda function to solve arithmetic double terms
 *
 */
template <>
inline std::function<double(const double&, const double&)> ArithmeticTerm<double>::get_operator(
    const std::string& string_op) {
  if (string_op == "+") {
    return std::plus<double>();
  } else if (string_op == "-") {
    return std::minus<double>();
  } else if (string_op == "*") {
    return std::multiplies<double>();
  } else if (string_op == "/") {
    return std::divides<double>();
  } else {
    throw std::runtime_error("Unknown arithmetic operator" + string_op);
  }
}

}  // namespace opossum
