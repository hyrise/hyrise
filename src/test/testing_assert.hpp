#pragma once

#include <cmath>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/abstract_operator.hpp"
#include "scheduler/operator_task.hpp"
#include "types.hpp"
#include "utils/check_table_equal.hpp"

namespace opossum {

class AbstractLQPNode;
class Table;

bool check_lqp_tie(const std::shared_ptr<const AbstractLQPNode>& output, LQPInputSide input_side,
                   const std::shared_ptr<const AbstractLQPNode>& input);

template <typename Functor>
bool contained_in_lqp(const std::shared_ptr<AbstractLQPNode>& node, Functor contains_fn) {
  if (!node) return false;
  if (contains_fn(node)) return true;
  return contained_in_lqp(node->left_input(), contains_fn) || contained_in_lqp(node->right_input(), contains_fn);
}

template <typename Functor>
bool contained_in_query_plan(const std::shared_ptr<const AbstractOperator>& node, Functor contains_fn) {
  if (!node) return false;
  if (contains_fn(node)) return true;
  return contained_in_query_plan(node->input_left(), contains_fn) ||
         contained_in_query_plan(node->input_right(), contains_fn);
}

}  // namespace opossum

/**
 * Compare two tables with respect to OrderSensitivity, TypeCmpMode and FloatComparisonMode
 */
#define EXPECT_TABLE_EQ(opossum_table, expected_table, order_sensitivity, type_cmp_mode, float_comparison_mode)    \
  EXPECT_TRUE(opossum_table&& expected_table&& check_table_equal(opossum_table, expected_table, order_sensitivity, \
                                                                 type_cmp_mode, float_comparison_mode));

/**
 * Specialised version of EXPECT_TABLE_EQ
 */
#define EXPECT_TABLE_EQ_UNORDERED(opossum_table, expected_table)                            \
  EXPECT_TABLE_EQ(opossum_table, expected_table, OrderSensitivity::No, TypeCmpMode::Strict, \
                  FloatComparisonMode::AbsoluteDifference)

/**
 * Specialised version of EXPECT_TABLE_EQ
 */
#define EXPECT_TABLE_EQ_ORDERED(opossum_table, expected_table)                               \
  EXPECT_TABLE_EQ(opossum_table, expected_table, OrderSensitivity::Yes, TypeCmpMode::Strict, \
                  FloatComparisonMode::AbsoluteDifference)

#define ASSERT_LQP_TIE(output, input_side, input)                   \
  {                                                                 \
    if (!opossum::check_lqp_tie(output, input_side, input)) FAIL(); \
  }                                                                 \
  static_assert(true, "End call of macro with a semicolon")

#define EXPECT_LQP_EQ(lhs, rhs)                                                                           \
  {                                                                                                       \
    Assert(lhs != rhs, "Comparing an LQP with itself is always true. Did you mean to take a deep copy?"); \
    const auto mismatch = lqp_find_subplan_mismatch(lhs, rhs);                                            \
    if (mismatch) {                                                                                       \
      std::cout << "Differing subtrees" << std::endl;                                                     \
      std::cout << "-------------- Actual LQP --------------" << std::endl;                               \
      if (mismatch->first)                                                                                \
        std::cout << *mismatch->first;                                                                    \
      else                                                                                                \
        std::cout << "NULL" << std::endl;                                                                 \
      std::cout << std::endl;                                                                             \
      std::cout << "------------- Expected LQP -------------" << std::endl;                               \
      if (mismatch->second)                                                                               \
        std::cout << *mismatch->second;                                                                   \
      else                                                                                                \
        std::cout << "NULL" << std::endl;                                                                 \
      std::cout << "-------------..............-------------" << std::endl;                               \
      GTEST_FAIL();                                                                                       \
    }                                                                                                     \
  }                                                                                                       \
  static_assert(true, "End call of macro with a semicolon")
