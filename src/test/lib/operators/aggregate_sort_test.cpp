#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "expression/aggregate_expression.hpp"
#include "operators/abstract_read_only_operator.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

void test_aggregate_output(const std::shared_ptr<AbstractOperator> in,
                           const std::vector<std::pair<ColumnID, AggregateFunction>>& aggregate_definitions,
                           const std::vector<ColumnID>& groupby_column_ids, const std::string& file_name) {
  // Load expected results from file.
  std::shared_ptr<Table> expected_result = load_table(file_name);

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{};
  const auto& table = in->get_output();
  for (const auto& [column_id, aggregate_function] : aggregate_definitions) {
    if (column_id != INVALID_COLUMN_ID) {
      aggregates.emplace_back(std::make_shared<AggregateExpression>(
          aggregate_function, pqp_column_(column_id, table->column_data_type(column_id),
                                          table->column_is_nullable(column_id), table->column_name(column_id))));
    } else {
      aggregates.emplace_back(std::make_shared<AggregateExpression>(
          aggregate_function, pqp_column_(column_id, DataType::Long, false, "*")));
    }
  }

  {
    // Test the Aggregate on stored table data.
    auto aggregate = std::make_shared<AggregateSort>(in, aggregates, groupby_column_ids);
    aggregate->execute();
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
  }

  {
    // Perform a TableScan to create a reference table
    const auto table_scan = std::make_shared<TableScan>(in, greater_than_(get_column_expression(in, ColumnID{0}), 0));
    table_scan->execute();

    // Perform the Aggregate on a reference table
    const auto aggregate = std::make_shared<AggregateSort>(table_scan, aggregates, groupby_column_ids);
    aggregate->execute();
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), expected_result);
  }
}

/**
 * Unsorted and unclustered data should be sufficiently tested with the general aggregate tests. This suite aims to
 * cover the parts related to exploiting sorting and clustering.
 */
class AggregateSortTest : public BaseTest {
 public:
  void SetUp() override {
    const auto table_1 = load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input.tbl", 2);
    table_1->set_value_clustered_by({ColumnID{0}});
    table_1->get_chunk(ChunkID{0})->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending));
    table_1->get_chunk(ChunkID{1})->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Descending));
    _table_wrapper_1 = std::make_shared<TableWrapper>(table_1);
    _table_wrapper_1->execute();

    const auto table_2 =
        load_table("resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/input_multi_columns.tbl", 2);
    table_2->set_value_clustered_by({ColumnID{0}, ColumnID{1}, ColumnID{2}});
    table_2->get_chunk(ChunkID{0})
        ->set_individually_sorted_by({SortColumnDefinition(ColumnID{0}, SortMode::Ascending),
                                      SortColumnDefinition(ColumnID{1}, SortMode::Ascending),
                                      SortColumnDefinition(ColumnID{2}, SortMode::Ascending)});
    table_2->get_chunk(ChunkID{1})
        ->set_individually_sorted_by({SortColumnDefinition(ColumnID{0}, SortMode::Descending),
                                      SortColumnDefinition(ColumnID{1}, SortMode::Descending),
                                      SortColumnDefinition(ColumnID{2}, SortMode::Ascending)});
    _table_wrapper_multi_columns = std::make_shared<TableWrapper>(table_2);
    _table_wrapper_multi_columns->execute();
  }

 protected:
  inline static std::shared_ptr<TableWrapper> _table_wrapper_1, _table_wrapper_multi_columns;
};

TEST_F(AggregateSortTest, SingleAggregateMaxSorted) {
  for (const auto& sort_by_column_id : {ColumnID{0}, ColumnID{1}}) {
    const auto sort = std::make_shared<Sort>(
        this->_table_wrapper_1, std::vector<SortColumnDefinition>{SortColumnDefinition{sort_by_column_id}});
    sort->execute();
    test_aggregate_output(sort, {{ColumnID{1}, AggregateFunction::Max}}, {ColumnID{0}},
                          "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/max.tbl");
  }
}

TEST_F(AggregateSortTest, SingleAggregateMinSorted) {
  for (const auto& sort_by_column_id : {ColumnID{0}, ColumnID{1}}) {
    const auto sort = std::make_shared<Sort>(
        this->_table_wrapper_1, std::vector<SortColumnDefinition>{SortColumnDefinition{sort_by_column_id}});
    sort->execute();
    test_aggregate_output(sort, {{ColumnID{1}, AggregateFunction::Min}}, {ColumnID{0}},
                          "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/min.tbl");
  }
}

TEST_F(AggregateSortTest, SingleAggregateSumSorted) {
  for (const auto& sort_by_column_id : {ColumnID{0}, ColumnID{1}}) {
    const auto sort = std::make_shared<Sort>(
        this->_table_wrapper_1, std::vector<SortColumnDefinition>{SortColumnDefinition{sort_by_column_id}});
    sort->execute();
    test_aggregate_output(sort, {{ColumnID{1}, AggregateFunction::Sum}}, {ColumnID{0}},
                          "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/sum.tbl");
  }
}

TEST_F(AggregateSortTest, SingleAggregateAvgSorted) {
  for (const auto& sort_by_column_id : {ColumnID{0}, ColumnID{1}}) {
    const auto sort = std::make_shared<Sort>(
        this->_table_wrapper_1, std::vector<SortColumnDefinition>{SortColumnDefinition{sort_by_column_id}});
    sort->execute();
    test_aggregate_output(sort, {{ColumnID{1}, AggregateFunction::Avg}}, {ColumnID{0}},
                          "resources/test_data/tbl/aggregateoperator/groupby_int_1gb_1agg/avg.tbl");
  }
}

TEST_F(AggregateSortTest, AggregateMaxMultiColumnSorted) {
  // Test for sorted by each column.
  const auto input_table = this->_table_wrapper_multi_columns->get_output();
  const auto column_count = input_table->column_count();
  for (auto sorted_by_column_id = ColumnID{0}; sorted_by_column_id < column_count; ++sorted_by_column_id) {
    const auto sort =
        std::make_shared<Sort>(this->_table_wrapper_multi_columns,
                               std::vector<SortColumnDefinition>{SortColumnDefinition{sorted_by_column_id}});
    sort->execute();
    // Group and aggregate by every column combination.
    for (auto group_by_column_id = ColumnID{0}; group_by_column_id < column_count; ++group_by_column_id) {
      for (auto aggregate_by_column_id = ColumnID{0}; aggregate_by_column_id < column_count; ++aggregate_by_column_id) {
        if (group_by_column_id == aggregate_by_column_id) continue;
        const auto table = sort->get_output();
        const auto aggregate_expressions = std::vector<std::shared_ptr<AggregateExpression>>{max_(pqp_column_(
            aggregate_by_column_id, table->column_data_type(aggregate_by_column_id),
            table->column_is_nullable(aggregate_by_column_id), table->column_name(aggregate_by_column_id)))};
        std::vector<ColumnID> groupby_column_ids = {group_by_column_id};
        const auto sorted_aggregate = std::make_shared<AggregateSort>(sort, aggregate_expressions, groupby_column_ids);
        const auto unsorted_aggregate = std::make_shared<AggregateHash>(this->_table_wrapper_multi_columns,
                                                                        aggregate_expressions, groupby_column_ids);
        sorted_aggregate->execute();
        unsorted_aggregate->execute();
        EXPECT_TABLE_EQ_UNORDERED(sorted_aggregate->get_output(), unsorted_aggregate->get_output());
      }
    }
  }
}

TEST_F(AggregateSortTest, AggregateOnPresortedValueClustered) {
  std::shared_ptr<Table> table_sorted_value_clustered =
      load_table("resources/test_data/tbl/int_sorted_value_clustered.tbl", 6);
  table_sorted_value_clustered->set_value_clustered_by({ColumnID{0}});

  const auto test_clustered_table_input = [&](const auto table) {
    const auto table_wrapper_sorted_value_clustered = std::make_shared<TableWrapper>(table);
    table_wrapper_sorted_value_clustered->execute();

    const auto aggregate_expression = std::vector<std::shared_ptr<AggregateExpression>>{
        sum_(pqp_column_(ColumnID{1}, table->column_data_type(ColumnID{1}), table->column_is_nullable(ColumnID{1}),
                         table->column_name(ColumnID{1})))};
    std::vector<ColumnID> groupby_column_ids = {ColumnID{0}};
    const auto aggregate =
        std::make_shared<AggregateSort>(table_wrapper_sorted_value_clustered, aggregate_expression, groupby_column_ids);
    aggregate->execute();

    const auto result_table = load_table("resources/test_data/tbl/int_sorted_value_clustered_result.tbl");
    EXPECT_TABLE_EQ_UNORDERED(aggregate->get_output(), result_table);
  };

  // Run aggregation on the value clustered table (each chunk will be sorted)
  test_clustered_table_input(table_sorted_value_clustered);
  test_clustered_table_input(to_simple_reference_table(table_sorted_value_clustered));

  // Run aggregation on the value clustered table with sorted flag (no sorting required)
  const auto chunk_count = table_sorted_value_clustered->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = table_sorted_value_clustered->get_chunk(chunk_id);
    chunk->set_individually_sorted_by(SortColumnDefinition(ColumnID{0}, SortMode::Ascending));
  }
  test_clustered_table_input(table_sorted_value_clustered);
  test_clustered_table_input(to_simple_reference_table(table_sorted_value_clustered));
}

}  // namespace opossum
