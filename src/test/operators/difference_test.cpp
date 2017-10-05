#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/difference.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {
class OperatorsDifferenceTest : public BaseTest {
 protected:
  virtual void SetUp() {
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));

    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float3.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
  }

  std::shared_ptr<TableWrapper> _table_wrapper_a;
  std::shared_ptr<TableWrapper> _table_wrapper_b;
};

TEST_F(OperatorsDifferenceTest, DifferenceOnValueTables) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 2);

  auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  difference->execute();

  EXPECT_TABLE_EQ(difference->get_output(), expected_result);
}

TEST_F(OperatorsDifferenceTest, DifferneceOnReferenceTables) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 2);

  Projection::ColumnExpressions column_expressions(
      {Expression::create_column(ColumnID{0}), Expression::create_column(ColumnID{1})});

  auto projection1 = std::make_shared<Projection>(_table_wrapper_a, column_expressions);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(_table_wrapper_b, column_expressions);
  projection2->execute();

  auto difference = std::make_shared<Difference>(projection1, projection2);
  difference->execute();

  EXPECT_TABLE_EQ(difference->get_output(), expected_result);
}

TEST_F(OperatorsDifferenceTest, ThrowWrongColumnNumberException) {
  if (!IS_DEBUG) return;
  auto table_wrapper_c = std::make_shared<TableWrapper>(load_table("src/test/tables/int.tbl", 2));
  table_wrapper_c->execute();

  auto difference = std::make_shared<Difference>(_table_wrapper_a, table_wrapper_c);

  EXPECT_THROW(difference->execute(), std::exception);
}

TEST_F(OperatorsDifferenceTest, ThrowWrongColumnOrderException) {
  if (!IS_DEBUG) return;
  _table_wrapper_a->execute();

  auto table_wrapper_d = std::make_shared<TableWrapper>(load_table("src/test/tables/float_int.tbl", 2));
  table_wrapper_d->execute();

  auto difference = std::make_shared<Difference>(_table_wrapper_a, table_wrapper_d);

  EXPECT_THROW(difference->execute(), std::exception);
}

}  // namespace opossum
