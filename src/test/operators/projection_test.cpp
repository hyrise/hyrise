#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_factory.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum::expression_factory;

namespace opossum {

/**
 * Projection mostly forwards its computations to the ExpressionEvaluator, so this the actual expression evaluation is
 * not tested here, but in the expression_evaluator_test.cpp
 */
class OperatorsProjectionTest : public BaseTest {
 public:
  void SetUp() override {
    table_wrapper = std::make_shared<TableWrapper>(load_table("src/test/tables/int_float.tbl", 2));
    table_wrapper->execute();

    a = PQPColumnExpression::from_table(*table_wrapper->get_output(), "a");
    b = PQPColumnExpression::from_table(*table_wrapper->get_output(), "b");
  }

  std::shared_ptr<TableWrapper> table_wrapper;
  std::shared_ptr<PQPColumnExpression> a, b;
};

TEST_F(OperatorsProjectionTest, OperatorName) {
  const auto projection = std::make_shared<opossum::Projection>(table_wrapper, expression_vector(a, b));
  EXPECT_EQ(projection->name(), "Projection");
}

TEST_F(OperatorsProjectionTest, ExecutedOnAllChunks) {
  const auto projection = std::make_shared<opossum::Projection>(table_wrapper, expression_vector(add(a, b)));
  projection->execute();
  EXPECT_TABLE_EQ_UNORDERED(projection->get_output(), load_table("src/test/tables/projection/int_float_add.tbl"));
}

TEST_F(OperatorsProjectionTest, ForwardsIfPossibleDataTable) {
  // The Projection will forward columns from its input if all expressions are column references. Why would you enforce
  // something like this? E.g., Update relies on it.

  const auto projection = std::make_shared<opossum::Projection>(table_wrapper, expression_vector(b, a));
  projection->execute();

  EXPECT_EQ(table_wrapper->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{1}), projection->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  EXPECT_EQ(table_wrapper->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{0}), projection->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{1}));
}

TEST_F(OperatorsProjectionTest, ForwardsIfPossibleReferenceTable) {
  // See ForwardsIfPossibleDataTable

  const auto table_scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, PredicateCondition::LessThan, 100'000);
  table_scan->execute();
  const auto projection = std::make_shared<opossum::Projection>(table_scan, expression_vector(b, a));
  projection->execute();

  EXPECT_EQ(table_scan->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{1}), projection->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{0}));
  EXPECT_EQ(table_scan->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{0}), projection->get_output()->get_chunk(ChunkID{0})->get_column(ColumnID{1}));
}

}  // namespace opossum
