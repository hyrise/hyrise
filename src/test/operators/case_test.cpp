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
    const auto table_a = load_table("src/test/tables/case/input_table.tbl", 2);
    _table_wrapper_a = std::make_shared<TableWrapper>(table_a);
    _table_wrapper_a->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_a;
};

TEST_F(CaseTest, WhenColumnThenValue) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN 142 [ELSE NULL]
   */

  const auto clause = PhysicalCaseClause<int32_t>(ColumnID{0}, int32_t{142});
  const auto case_expression = std::make_shared<PhysicalCaseExpression<int32_t>>(clause, Null{}, "c0");

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, Case::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(), load_table("src/test/tables/case/when_column_then_value.tbl"));
}

TEST_F(CaseTest, WhenColumnThenColumnElseColumn) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN #Col1 ELSE #Col2
   */

  const auto clause = PhysicalCaseClause<int32_t>(ColumnID{0}, ColumnID{1});
  const auto case_expression = std::make_shared<PhysicalCaseExpression<int32_t>>(clause, ColumnID{2}, "c0");

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, Case::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(), load_table("src/test/tables/case/when_column_then_column_else_column.tbl"));
}

TEST_F(CaseTest, WhenColumnThenNullElseValue) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN NULL ELSE 143
   */

  const auto clause = PhysicalCaseClause<int32_t>(ColumnID{0}, Null{});
  const auto case_expression =  std::make_shared<PhysicalCaseExpression<int32_t>>(clause, 143, "c0");

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, Case::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(), load_table("src/test/tables/case/when_column_then_null_else_column.tbl"));
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

  auto clauses = std::vector<PhysicalCaseClause<float>>{};
  clauses.emplace_back(ColumnID{0}, ColumnID{3});
  clauses.emplace_back(ColumnID{4}, float{3.14});
  const auto case_expression =  std::make_shared<PhysicalCaseExpression<float>>(clauses, 13.0f, "c0");

  const auto case_op = std::make_shared<Case>(_table_wrapper_a, Case::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(), load_table("src/test/tables/case/when_columns_then_mixed_else_value.tbl"));
}

}  // namespace opossum
