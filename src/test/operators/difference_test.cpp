#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/difference.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {
class OperatorsDifferenceTest : public BaseTest {
 protected:
  virtual void SetUp() {
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));

    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float3.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_a;
  std::shared_ptr<TableWrapper> _table_wrapper_b;
};

TEST_F(OperatorsDifferenceTest, DifferenceOnValueTables) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", 2);

  auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  difference->execute();

  EXPECT_TABLE_EQ_UNORDERED(difference->get_output(), expected_result);
}

TEST_F(OperatorsDifferenceTest, DifferneceOnReferenceTables) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered2.tbl", 2);

  const auto a = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "a");
  const auto b = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "b");

  auto projection1 = std::make_shared<Projection>(_table_wrapper_a, expression_vector(a, b));
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(_table_wrapper_b, expression_vector(a, b));
  projection2->execute();

  auto difference = std::make_shared<Difference>(projection1, projection2);
  difference->execute();

  EXPECT_TABLE_EQ_UNORDERED(difference->get_output(), expected_result);
}

TEST_F(OperatorsDifferenceTest, ThrowWrongColumnNumberException) {
  if (!HYRISE_DEBUG) GTEST_SKIP();
  auto table_wrapper_c = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int.tbl", 2));
  table_wrapper_c->execute();

  auto difference = std::make_shared<Difference>(_table_wrapper_a, table_wrapper_c);

  EXPECT_THROW(difference->execute(), std::exception);
}

TEST_F(OperatorsDifferenceTest, ThrowWrongColumnOrderException) {
  if (!HYRISE_DEBUG) GTEST_SKIP();

  auto table_wrapper_d = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/float_int.tbl", 2));
  table_wrapper_d->execute();

  auto difference = std::make_shared<Difference>(_table_wrapper_a, table_wrapper_d);

  EXPECT_THROW(difference->execute(), std::exception);
}

}  // namespace opossum
