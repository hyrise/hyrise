#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "gtest/gtest.h"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/product.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {
class OperatorsProductTest : public BaseTest {
 public:
  std::shared_ptr<opossum::TableWrapper> _table_wrapper_a, _table_wrapper_b, _table_wrapper_c;

  virtual void SetUp() {
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int.tbl", 5));
    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/float.tbl", 2));
    _table_wrapper_c = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_int.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
    _table_wrapper_c->execute();
  }
};

TEST_F(OperatorsProductTest, ValueSegments) {
  auto product = std::make_shared<Product>(_table_wrapper_a, _table_wrapper_b);
  product->execute();

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_product.tbl", 3);
  EXPECT_TABLE_EQ_UNORDERED(product->get_output(), expected_result);
}

TEST_F(OperatorsProductTest, ReferenceAndValueSegments) {
  auto table_scan = create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
  table_scan->execute();

  auto product = std::make_shared<Product>(table_scan, _table_wrapper_b);
  product->execute();

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_filtered_float_product.tbl", 3);
  EXPECT_TABLE_EQ_UNORDERED(product->get_output(), expected_result);
}

TEST_F(OperatorsProductTest, SelfProduct) {
  auto product = std::make_shared<Product>(_table_wrapper_c, _table_wrapper_c);
  product->execute();

  const auto expected_result = load_table("resources/test_data/tbl/int_int_self_product.tbl");
  EXPECT_TABLE_EQ_UNORDERED(product->get_output(), expected_result);
}

}  // namespace opossum
