#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/difference.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {
class OperatorsDifferenceTest : public BaseTest {
 protected:
  void SetUp() override {
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

TEST_F(OperatorsDifferenceTest, ForwardSortedByFlag) {
  // Verify that the sorted_by flag is not set when it's not present in left input.
  const auto difference_unsorted = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  difference_unsorted->execute();

  const auto& result_table_unsorted = difference_unsorted->get_output();
  for (auto chunk_id = ChunkID{0}; chunk_id < result_table_unsorted->chunk_count(); ++chunk_id) {
    const auto& sorted_by = result_table_unsorted->get_chunk(chunk_id)->individually_sorted_by();
    EXPECT_TRUE(sorted_by.empty());
  }

  // Verify that the sorted_by flag is set when it's present in left input.
  const auto sort_definition = std::vector<SortColumnDefinition>{SortColumnDefinition(ColumnID{0})};
  const auto sort = std::make_shared<Sort>(_table_wrapper_a, sort_definition);
  sort->execute();

  const auto difference_sorted = std::make_shared<Difference>(sort, _table_wrapper_b);
  difference_sorted->execute();

  const auto& result_table_sorted = difference_sorted->get_output();
  for (auto chunk_id = ChunkID{0}; chunk_id < result_table_sorted->chunk_count(); ++chunk_id) {
    const auto sorted_by = result_table_sorted->get_chunk(chunk_id)->individually_sorted_by();
    EXPECT_EQ(sorted_by, sort_definition);
  }
}

}  // namespace opossum
