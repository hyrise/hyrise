#include "base_test.hpp"
#include "gtest/gtest.h"
#include "operators/table_wrapper.hpp"

namespace opossum {

class OperatorsTableScanSortedTest : public BaseTest, public ::testing::WithParamInterface<EncodingType> {
 protected:
  void SetUp() override {
    _table_column_definitions.emplace_back("ascending", DataType::Int, true);
    _table_column_definitions.emplace_back("ascending_nulls_last", DataType::Int, true);
    _table_column_definitions.emplace_back("descending", DataType::Int, true);
    _table_column_definitions.emplace_back("descending_nulls_last", DataType::Int, true);

    _table_column_definitions.emplace_back("ascending", DataType::String, true);
    _table_column_definitions.emplace_back("ascending_nulls_last", DataType::String, true);
    _table_column_definitions.emplace_back("descending", DataType::String, true);
    _table_column_definitions.emplace_back("descending_nulls_last", DataType::String, true);

    _table = std::make_shared<Table>(_table_column_definitions, TableType::Data);

    const int table_size = 10;
    for (int i = 0; i < table_size; i++) {
      const auto str1 = std::to_string(i);
      const auto str2 = std::to_string(table_size - i - 1);
      _table->append({i, i, table_size - i - 1, table_size - i - 1, str1, str1, str2, str2});
    }

    for (auto i = 0; i < 2; i++) {
      _table->get_chunk(ChunkID(0))->get_segment(ColumnID(0 + i * 4))->set_sort_order(OrderByMode::Ascending);
      _table->get_chunk(ChunkID(0))->get_segment(ColumnID(1 + i * 4))->set_sort_order(OrderByMode::AscendingNullsLast);
      _table->get_chunk(ChunkID(0))->get_segment(ColumnID(2 + i * 4))->set_sort_order(OrderByMode::Descending);
      _table->get_chunk(ChunkID(0))->get_segment(ColumnID(3 + i * 4))->set_sort_order(OrderByMode::DescendingNullsLast);
    }

    _table_wrapper = std::make_shared<TableWrapper>(std::move(_table));
    _table_wrapper->execute();
  }

  void ASSERT_COLUMN_SORTED_EQ(std::shared_ptr<const Table> table, const ColumnID& column_id,
                               std::vector<AllTypeVariant> expected) {
    for (auto chunk_id = ChunkID{0u}; chunk_id < table->chunk_count(); ++chunk_id) {
      const auto chunk = table->get_chunk(chunk_id);

      for (auto chunk_offset = ChunkOffset{0u}; chunk_offset < chunk->size(); ++chunk_offset) {
        const auto& segment = *chunk->get_segment(column_id);

        const auto found_value = segment[chunk_offset];

        ASSERT_TRUE(expected[0] == found_value);
        expected.erase(expected.cbegin());
      }
    }

    ASSERT_EQ(expected.size(), 0u);
  }

 protected:
  std::shared_ptr<Table> _table;
  std::shared_ptr<TableWrapper> _table_wrapper;
  TableColumnDefinitions _table_column_definitions;
};

TEST_F(OperatorsTableScanSortedTest, TestAscendingScanInt) {
  const AllTypeVariant value = 5;

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {5};
  tests[PredicateCondition::NotEquals] = {0, 1, 2, 3, 4, 6, 7, 8, 9};
  tests[PredicateCondition::LessThan] = {0, 1, 2, 3, 4};
  tests[PredicateCondition::LessThanEquals] = {0, 1, 2, 3, 4, 5};
  tests[PredicateCondition::GreaterThan] = {6, 7, 8, 9};
  tests[PredicateCondition::GreaterThanEquals] = {5, 6, 7, 8, 9};

  for (const auto& test : tests) {
    for (auto column_index = ColumnID{0}; column_index < 2; ++column_index) {
      const auto column_definition = _table_column_definitions[column_index];
      const auto column_expression =
          pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

      auto predicate = std::make_shared<BinaryPredicateExpression>(test.first, column_expression, value_(value));
      auto scan = std::make_shared<TableScan>(_table_wrapper, predicate);
      scan->execute();

      ASSERT_COLUMN_SORTED_EQ(scan->get_output(), column_index, test.second);
    }
  }
}

TEST_F(OperatorsTableScanSortedTest, TestDescendingScanInt) {
  const AllTypeVariant value = 5;

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {5};
  tests[PredicateCondition::NotEquals] = {9, 8, 7, 6, 4, 3, 2, 1, 0};
  tests[PredicateCondition::LessThan] = {4, 3, 2, 1, 0};
  tests[PredicateCondition::LessThanEquals] = {5, 4, 3, 2, 1, 0};
  tests[PredicateCondition::GreaterThan] = {9, 8, 7, 6};
  tests[PredicateCondition::GreaterThanEquals] = {9, 8, 7, 6, 5};

  for (const auto& test : tests) {
    for (auto column_index = ColumnID{2}; column_index < 4; ++column_index) {
      const auto column_definition = _table_column_definitions[column_index];
      const auto column_expression =
          pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

      auto predicate = std::make_shared<BinaryPredicateExpression>(test.first, column_expression, value_(value));
      auto scan = std::make_shared<TableScan>(_table_wrapper, predicate);
      scan->execute();

      ASSERT_COLUMN_SORTED_EQ(scan->get_output(), column_index, test.second);
    }
  }
}

TEST_F(OperatorsTableScanSortedTest, TestAscendingScanString) {
  const AllTypeVariant value = "5";

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {"5"};
  tests[PredicateCondition::NotEquals] = {"0", "1", "2", "3", "4", "6", "7", "8", "9"};
  tests[PredicateCondition::LessThan] = {"0", "1", "2", "3", "4"};
  tests[PredicateCondition::LessThanEquals] = {"0", "1", "2", "3", "4", "5"};
  tests[PredicateCondition::GreaterThan] = {"6", "7", "8", "9"};
  tests[PredicateCondition::GreaterThanEquals] = {"5", "6", "7", "8", "9"};

  for (const auto& test : tests) {
    for (auto column_index = ColumnID{4}; column_index < 6; ++column_index) {
      const auto column_definition = _table_column_definitions[column_index];
      const auto column_expression =
          pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

      auto predicate = std::make_shared<BinaryPredicateExpression>(test.first, column_expression, value_(value));
      auto scan = std::make_shared<TableScan>(_table_wrapper, predicate);
      scan->execute();

      ASSERT_COLUMN_SORTED_EQ(scan->get_output(), column_index, test.second);
    }
  }
}

TEST_F(OperatorsTableScanSortedTest, TestDescendingScanString) {
  const AllTypeVariant value = "5";

  std::map<PredicateCondition, std::vector<AllTypeVariant>> tests;
  tests[PredicateCondition::Equals] = {"5"};
  tests[PredicateCondition::NotEquals] = {"9", "8", "7", "6", "4", "3", "2", "1", "0"};
  tests[PredicateCondition::LessThan] = {"4", "3", "2", "1", "0"};
  tests[PredicateCondition::LessThanEquals] = {"5", "4", "3", "2", "1", "0"};
  tests[PredicateCondition::GreaterThan] = {"9", "8", "7", "6"};
  tests[PredicateCondition::GreaterThanEquals] = {"9", "8", "7", "6", "5"};

  for (const auto& test : tests) {
    for (auto column_index = ColumnID{6}; column_index < 8; ++column_index) {
      const auto column_definition = _table_column_definitions[column_index];
      const auto column_expression =
          pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

      auto predicate = std::make_shared<BinaryPredicateExpression>(test.first, column_expression, value_(value));
      auto scan = std::make_shared<TableScan>(_table_wrapper, predicate);
      scan->execute();

      ASSERT_COLUMN_SORTED_EQ(scan->get_output(), column_index, test.second);
    }
  }
}

}  // namespace opossum
