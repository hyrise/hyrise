#include <iostream>

#include "types.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "operators/get_table.hpp"
#include "operators/aggregate_verification.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/print.hpp"


using namespace opossum;  // NOLINT

int main() {
  TpchTableGenerator{0.001f, 250}.generate_and_store();

  const auto lineitem_op = std::make_shared<GetTable>("lineitem");
  lineitem_op->execute();

  auto aggregates = std::vector<AggregateColumnDefinition>{};
  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Min);
  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Max);
  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Avg);
  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Sum);
  aggregates.emplace_back(ColumnID{5}, AggregateFunction::Count);
  aggregates.emplace_back(std::nullopt, AggregateFunction::Count);
  aggregates.emplace_back(ColumnID{5}, AggregateFunction::CountDistinct);

  auto group_by_column_ids = std::vector<ColumnID>{};
  group_by_column_ids.emplace_back(ColumnID{13});
  group_by_column_ids.emplace_back(ColumnID{14});

  const auto aggregate_op = std::make_shared<AggregateVerification>(lineitem_op, aggregates, group_by_column_ids);
  aggregate_op->execute();

  Print::print(aggregate_op->get_output());


  return 0;
}
