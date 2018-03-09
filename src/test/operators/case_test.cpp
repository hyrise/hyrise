#include "gtest/gtest.h"

#include "types.hpp"

#include "operators/case_operator.hpp"
#include "operators/table_wrapper.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

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

  const auto clause = PhysicalCaseWhenClause<int32_t>(ColumnID{0}, int32_t{142});
  const auto case_expression = std::make_shared<PhysicalCaseExpression<int32_t>>(clause, Null{});

  const auto case_op = std::make_shared<CaseOperator>(_table_wrapper_a, CaseOperator::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(), load_table("src/test/tables/case/when_column_then_value.tbl"));
}

TEST_F(CaseTest, WhenColumnThenColumnElseColumn) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN #Col1 ELSE #Col2
   */

  const auto clause = PhysicalCaseWhenClause<int32_t>(ColumnID{0}, ColumnID{1});
  const auto case_expression = std::make_shared<PhysicalCaseExpression<int32_t>>(clause, ColumnID{2});

  const auto case_op = std::make_shared<CaseOperator>(_table_wrapper_a, CaseOperator::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(),
                            load_table("src/test/tables/case/when_column_then_column_else_column.tbl"));
}

TEST_F(CaseTest, WhenColumnThenNullElseValue) {
  /**
   * Test
   *    CASE WHEN #Col0 THEN NULL ELSE 143
   */

  const auto clause = PhysicalCaseWhenClause<int32_t>(ColumnID{0}, Null{});
  const auto case_expression = std::make_shared<PhysicalCaseExpression<int32_t>>(clause, 143);

  const auto case_op = std::make_shared<CaseOperator>(_table_wrapper_a, CaseOperator::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(),
                            load_table("src/test/tables/case/when_column_then_null_else_value.tbl"));
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

  auto clauses = std::vector<PhysicalCaseWhenClause<float>>{};
  clauses.emplace_back(ColumnID{0}, ColumnID{3});
  clauses.emplace_back(ColumnID{4}, float{3.14});
  const auto case_expression = std::make_shared<PhysicalCaseExpression<float>>(clauses, 13.0f);

  const auto case_op = std::make_shared<CaseOperator>(_table_wrapper_a, CaseOperator::Expressions{case_expression});
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(),
                            load_table("src/test/tables/case/when_columns_then_mixed_else_value.tbl"));
}

TEST_F(CaseTest, MultipleExpressionsAndDescription) {
  /**
   * Test multiple expressions (std::string and int64_t, just to have them covered as well)
   *    CASE
   *        WHEN #Col0 THEN "Hallo"
   *        WHEN #Col4 THEN "Welt"
   *        ELSE "!"
   *    END
   *    ,
   *    CASE
   *        WHEN #Col4 THEN 42
   *        WHEN #Col0 THEN 43
   *    END
   */

  CaseOperator::Expressions expressions;

  // First expression
  auto clauses_a = std::vector<PhysicalCaseWhenClause<std::string>>{};
  clauses_a.emplace_back(ColumnID{0}, std::string("Hallo"));
  clauses_a.emplace_back(ColumnID{4}, std::string("Welt"));
  expressions.emplace_back(std::make_shared<PhysicalCaseExpression<std::string>>(clauses_a, std::string("!")));

  // Second expression
  auto clauses_b = std::vector<PhysicalCaseWhenClause<int64_t>>{};
  clauses_b.emplace_back(ColumnID{4}, int64_t{42});
  clauses_b.emplace_back(ColumnID{0}, int64_t{43});
  expressions.emplace_back(std::make_shared<PhysicalCaseExpression<int64_t>>(clauses_b, Null()));

  //
  const auto case_op = std::make_shared<CaseOperator>(_table_wrapper_a, expressions);
  case_op->execute();

  EXPECT_TABLE_EQ_UNORDERED(case_op->get_output(), load_table("src/test/tables/case/multiple_expressions.tbl"));

  /**
   * Test description here as well, so we don't have to build a "complex" Case operator in multiple places
   */
  EXPECT_EQ(
      case_op->description(DescriptionMode::SingleLine),
      R"(CASE {[WHEN #Col0 THEN 'Hallo', WHEN #Col4 THEN 'Welt', ELSE '!'], [WHEN #Col4 THEN '42', WHEN #Col0 THEN '43', ELSE NULL]})");
  EXPECT_EQ(case_op->description(DescriptionMode::MultiLine), R"(CASE {
     [WHEN #Col0 THEN 'Hallo', WHEN #Col4 THEN 'Welt', ELSE '!']
     [WHEN #Col4 THEN '42', WHEN #Col0 THEN '43', ELSE NULL]})");
}

}  // namespace opossum
