#include "gtest/gtest.h"

#include "types.hpp"

#include "operators/case.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

namespace opossum {

class CaseTest : public ::testing::Test {
 public:
  void SetUp() override {

  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_a;
};

TEST_F(CaseTest, WhenColumnThenValue) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN 42 ELSE NULL
   */

  const auto clause = PhysicalCaseClause<int32_t>(ColumnID{0}, int32_t{42});
  const auto case_definition = PhysicalCaseExpression<int32_t>({clause});

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, case_definition);
  case_op->execute();

  EXPECT_TABLE_EQ(case_op->get_output(), load_table("src/test/case/when_column_then_value.tbl"));
}

TEST_F(CaseTest, WhenColumnThenColumnElseColumn) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN #Col1 ELSE #Col2
   */

  const auto clause = PhysicalCaseClause<int32_t>(ColumnID{0}, ColumnID{1});
  const auto case_definition = PhysicalCaseExpression<int32_t>({clause}, ColumnID{2});

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, case_definition);
  case_op->execute();

  EXPECT_TABLE_EQ(case_op->get_output(), load_table("src/test/case/when_column_then_column_else_column.tbl"));
}

TEST_F(CaseTest, WhenColumnThenNullElseValue) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN NULL ELSE #Col2
   */

  const auto clause = PhysicalCaseClause<int32_t>(ColumnID{0}, Null{});
  const auto case_definition = PhysicalCaseExpression<int32_t>({clause}, 42);

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, case_definition);
  case_op->execute();

  EXPECT_TABLE_EQ(case_op->get_output(), load_table("src/test/case/when_column_then_null_else_column.tbl"));
}

TEST_F(CaseTest, WhenColumnsThenMixedElseValue) {
  /**
   * Test
   *    CASE
   *        WHEN #Col0 THEN #Col3
   *        WHEN #Col4 THEN 3.14
   *        ELSE 13
   *    END
   */

  const auto clauses = std::vector<PhysicalCaseClause<float>>{};
  clauses.emplace_back(ColumnID{0}, ColumnID{3});
  clauses.emplace_back(ColumnID{4}, float{3.14});
  const auto case_definition = PhysicalCaseExpression<float>(clauses, float{13});

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, case_definition);
  case_op->execute();

  EXPECT_TABLE_EQ(case_op->get_output(), load_table("src/test/case/when_columns_then_mixed_else_value.tbl"));
}

}  // namespace opossum
