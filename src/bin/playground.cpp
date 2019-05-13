#include <iostream>

#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_verification.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
//  TpchTableGenerator{0.001f, 250}.generate_and_store();

//  const auto table_op = std::make_shared<GetTable>("lineitem");
//  table_op->execute();

//  auto aggregates = std::vector<AggregateColumnDefinition>{};
//  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Min);
//  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Max);
//  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Avg);
//  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Sum);
//  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Count);
//  aggregates.emplace_back(std::nullopt, AggregateFunction::Count);
//  aggregates.emplace_back(ColumnID{5}, AggregateFunction::CountDistinct);

//  auto group_by_column_ids = std::vector<ColumnID>{};
//  group_by_column_ids.emplace_back(ColumnID{13});
//  group_by_column_ids.emplace_back(ColumnID{14});

  TableColumnDefinitions table_column_definitions;
  table_column_definitions.emplace_back("a", DataType::Int, true);
  table_column_definitions.emplace_back("b", DataType::Int, true);

  const auto table = std::make_shared<Table>(table_column_definitions, TableType::Data);
  table->append({NullValue{}, 5});
  table->append({NullValue{}, 6});
  table->append({3, 6});

  const auto table_op = std::make_shared<TableWrapper>(table);
  table_op->execute();

  auto aggregates = std::vector<AggregateColumnDefinition>{};
  aggregates.emplace_back(ColumnID{1}, AggregateFunction::Sum);

  auto group_by_column_ids = std::vector<ColumnID>{};
  group_by_column_ids.emplace_back(ColumnID{0});

  const auto aggregate_op = std::make_shared<AggregateVerification>(table_op, aggregates, group_by_column_ids);
  aggregate_op->execute();

  Print::print(aggregate_op->get_output());

  std::cout << (NullValue{} > NullValue{}) << std::endl;
  std::cout << (NullValue{} < NullValue{}) << std::endl;
  std::cout << (NullValue{} == NullValue{}) << std::endl;

  return 0;
}
