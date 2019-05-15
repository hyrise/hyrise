#include <iostream>

#include "types.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "operators/get_table.hpp"
#include "operators/aggregate_hashsort.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "storage/table.hpp"


using namespace opossum;  // NOLINT

int main() {
  TableColumnDefinitions table_column_definitions;
  table_column_definitions.emplace_back("a", DataType::Int);
  table_column_definitions.emplace_back("b", DataType::Int);

  const auto table = std::make_shared<Table>(table_column_definitions, TableType::Data);
  table->append({1, 2});
  table->append({2, 5});
  table->append({1, 3});
  table->append({2, 6});

  const auto table_op = std::make_shared<TableWrapper>(table);
  table_op->execute();

  auto aggregates = std::vector<AggregateColumnDefinition>{};
  //aggregates.emplace_back(ColumnID{1}, AggregateFunction::Sum);

  auto group_by_column_ids = std::vector<ColumnID>{};
  group_by_column_ids.emplace_back(ColumnID{0});

  const auto aggregate_op = std::make_shared<AggregateHashSort>(table_op, aggregates, group_by_column_ids);
  aggregate_op->execute();

  Print::print(aggregate_op->get_output());


  return 0;
}
