#pragma once

#include <cmath>
#include <memory>
#include <string>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/abstract_operator.hpp"
#include "scheduler/operator_task.hpp"
#include "types.hpp"
#include "utils/check_table_equal.hpp"

namespace hyrise {

class AbstractLQPNode;
class Table;

bool check_lqp_tie(const std::shared_ptr<const AbstractLQPNode>& output, LQPInputSide input_side,
                   const std::shared_ptr<const AbstractLQPNode>& input);

template <typename Functor>
bool contained_in_lqp(const std::shared_ptr<AbstractLQPNode>& node, Functor contains_fn) {
  if (!node) {
    return false;
  }

  if (contains_fn(node)) {
    return true;
  }

  return contained_in_lqp(node->left_input(), contains_fn) || contained_in_lqp(node->right_input(), contains_fn);
}

template <typename Functor>
bool contained_in_query_plan(const std::shared_ptr<const AbstractOperator>& node, Functor contains_fn) {
  if (!node) {
    return false;
  }

  if (contains_fn(node)) {
    return true;
  }

  return contained_in_query_plan(node->left_input(), contains_fn) ||
         contained_in_query_plan(node->right_input(), contains_fn);
}

}  // namespace hyrise

/**
 * Compare two tables with respect to OrderSensitivity, TypeCmpMode and FloatComparisonMode
 */
#define EXPECT_TABLE_EQ(hyrise_table, expected_table, order_sensitivity, type_cmp_mode, float_comparison_mode)       \
  {                                                                                                                  \
    if (const auto table_difference_message =                                                                        \
            check_table_equal(hyrise_table, expected_table, order_sensitivity, type_cmp_mode, float_comparison_mode, \
                              IgnoreNullable::No)) {                                                                 \
      FAIL() << *table_difference_message;                                                                           \
    }                                                                                                                \
  }                                                                                                                  \
  static_assert(true, "End call of macro with a semicolon")

/**
 * Specialised version of EXPECT_TABLE_EQ
 */
#define EXPECT_TABLE_EQ_UNORDERED(hyrise_table, expected_table)                            \
  EXPECT_TABLE_EQ(hyrise_table, expected_table, OrderSensitivity::No, TypeCmpMode::Strict, \
                  FloatComparisonMode::AbsoluteDifference)

/**
 * Specialised version of EXPECT_TABLE_EQ
 */
#define EXPECT_TABLE_EQ_ORDERED(hyrise_table, expected_table)                               \
  EXPECT_TABLE_EQ(hyrise_table, expected_table, OrderSensitivity::Yes, TypeCmpMode::Strict, \
                  FloatComparisonMode::AbsoluteDifference)

/**
 * Compare two segments with respect to OrderSensitivity, TypeCmpMode and FloatComparisonMode
 */
// clang-format off
#define EXPECT_SEGMENT_EQ(segment_to_test, expected_segment, order_sensitivity, type_cmp_mode, float_comparison_mode) \
  EXPECT_TRUE(segment_to_test && expected_segment && check_segment_equal(                                             \
              segment_to_test, expected_segment, order_sensitivity, type_cmp_mode, float_comparison_mode))
// clang-format on

/**
 * Specialised version of EXPECT_SEGMENT_EQ
 */
#define EXPECT_SEGMENT_EQ_UNORDERED(segment_to_test, expected_segment)                            \
  EXPECT_SEGMENT_EQ(segment_to_test, expected_segment, OrderSensitivity::No, TypeCmpMode::Strict, \
                    FloatComparisonMode::AbsoluteDifference)

/**
 * Specialised version of EXPECT_SEGMENT_EQ
 */
#define EXPECT_SEGMENT_EQ_ORDERED(segment_to_test, expected_segment)                               \
  EXPECT_SEGMENT_EQ(segment_to_test, expected_segment, OrderSensitivity::Yes, TypeCmpMode::Strict, \
                    FloatComparisonMode::AbsoluteDifference)

#define ASSERT_LQP_TIE(output, input_side, input)            \
  {                                                          \
    if (!hyrise::check_lqp_tie(output, input_side, input)) { \
      FAIL();                                                \
    }                                                        \
  }                                                          \
  static_assert(true, "End call of macro with a semicolon")

#define EXPECT_LQP_EQ(lhs, rhs)                                                                              \
  {                                                                                                          \
    Assert(std::static_pointer_cast<AbstractLQPNode>(lhs) != std::static_pointer_cast<AbstractLQPNode>(rhs), \
           "Comparing an LQP with itself is always true. Did you mean to take a deep copy?");                \
    const auto mismatch = lqp_find_subplan_mismatch(lhs, rhs);                                               \
    if (mismatch) {                                                                                          \
      std::cerr << "Differing subtrees\n";                                                                   \
      std::cerr << "-------------- Actual LQP --------------\n";                                             \
      if (mismatch->first) {                                                                                 \
        std::cerr << *mismatch->first;                                                                       \
      } else {                                                                                               \
        std::cerr << "NULL\n";                                                                               \
      }                                                                                                      \
      std::cerr << "\n------------- Expected LQP -------------\n";                                           \
      if (mismatch->second) {                                                                                \
        std::cerr << *mismatch->second;                                                                      \
      } else {                                                                                               \
        std::cerr << "NULL\n";                                                                               \
      }                                                                                                      \
      std::cout << "-------------..............-------------\n";                                             \
      GTEST_FAIL();                                                                                          \
    }                                                                                                        \
  }                                                                                                          \
  static_assert(true, "End call of macro with a semicolon")

#define EXPECT_TASKS_EQ(lhs, rhs)                                 \
  {                                                               \
    ASSERT_EQ(lhs.size(), rhs.size());                            \
                                                                  \
    for (auto index = size_t{0}; index < lhs.size(); ++index) {   \
      EXPECT_EQ(lhs[index].get().shared_from_this(), rhs[index]); \
    }                                                             \
  }                                                               \
  static_assert(true, "End call of macro with a semicolon")
