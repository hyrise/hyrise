#include "gtest/gtest.h"

#include "expression/case_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum::expression_factory;  // NOLINT

namespace opossum {

class ExpressionTest : public ::testing::Test {
 public:
  void SetUp() {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));

    const auto int_float_node_a = StoredTableNode::make("int_float");
    const auto a = LQPColumnReference{int_float_node_a, ColumnID{0}};
    const auto b = LQPColumnReference{int_float_node_a, ColumnID{1}};

    case_a = case_(equals(add(a, 5), b), add(5, b), a);
    case_b = case_(a, 1, 3);
    case_c = case_(equals(a, 123), b, case_(equals(a, 1234), a, null()));
  }

  std::shared_ptr<AbstractExpression> case_a, case_b, case_c;
};

TEST_F(ExpressionTest, DeepEquals) {
  EXPECT_TRUE(case_a->deep_equals(*case_a));
  const auto case_a_copy = case_a->deep_copy();
  EXPECT_TRUE(case_a->deep_equals(*case_a_copy));
  EXPECT_TRUE(case_c->deep_equals(*case_c));
  EXPECT_FALSE(case_a->deep_equals(*case_b));
  EXPECT_FALSE(case_a->deep_equals(*case_c));

  //const auto case_c
}

}  // namespace opossum
