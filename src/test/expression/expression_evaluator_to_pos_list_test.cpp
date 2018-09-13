#include <optional>

#include "gtest/gtest.h"

#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExpressionEvaluatorToPosListTest : public ::testing::Test {
 public:
  void SetUp() override {
    // Load table_b
    table_b = load_table("src/test/tables/expression_evaluator/input_b.tbl", 4);
    x = PQPColumnExpression::from_table(*table_b, "x");
  }

  bool test_expression(const std::shared_ptr<Table>& table, const ChunkID chunk_id, const AbstractExpression& expression, const std::vector<ChunkOffset>& matching_chunk_offsets) {
    const auto actual_pos_list = ExpressionEvaluator{table, chunk_id}.evaluate_expression_to_pos_list(expression);

    auto expected_pos_list = PosList{};
    expected_pos_list.resize(matching_chunk_offsets.size());
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < matching_chunk_offsets.size(); ++chunk_offset) {
      expected_pos_list[chunk_offset] = RowID{chunk_id, matching_chunk_offsets[chunk_offset]};
    }

    return actual_pos_list == expected_pos_list;
  }

  std::shared_ptr<Table> table_b;

  std::shared_ptr<PQPColumnExpression> x;
};

TEST_F(ExpressionEvaluatorToPosListTest, Predicate) {
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_(x, 9), {0, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *greater_than_(x, 9), {}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{0}, *greater_than_equals_(x, 9), {0, 1, 2}));
  EXPECT_TRUE(test_expression(table_b, ChunkID{1}, *greater_than_equals_(x, 8), {1, 3}));
}

}  // namespace opossum