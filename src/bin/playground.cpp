#include <iostream>

#include <random>

#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_hashsort.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/get_table.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "types.hpp"
#include "utils/timer.hpp"

using namespace opossum;  // NOLINT

template<typename T>
std::shared_ptr<ValueSegment<T>> make_int_segment(size_t row_count, int32_t min, int32_t max) {
  std::uniform_int_distribution<T> dist{min, max};
  std::random_device rd;
  std::mt19937 gen(rd());

  std::vector<T> values(row_count);

  for (auto i = size_t{0}; i < row_count; ++i) {
    values[i] = dist(gen);
  }

  return std::make_shared<ValueSegment<T>>(std::move(values));
}

std::shared_ptr<ValueSegment<pmr_string>> make_date_segment(size_t row_count, int32_t min, int32_t max) {
  std::uniform_int_distribution<int32_t> dist{min, max};
  std::random_device rd;
  std::mt19937 gen(rd());

  std::vector<pmr_string> values(row_count);

  for (auto i = size_t{0}; i < row_count; ++i) {
    const auto s = std::to_string(dist(gen));
    values[i] = s + s;
  }

  return std::make_shared<ValueSegment<pmr_string>>(std::move(values));
}

std::shared_ptr<Table> make_number_table() {
  TableColumnDefinitions table_column_definitions;
  table_column_definitions.emplace_back("a", DataType::Int);
  table_column_definitions.emplace_back("b", DataType::Int);
  table_column_definitions.emplace_back("c", DataType::Int);

  const auto table = std::make_shared<Table>(table_column_definitions, TableType::Data);
  //  table->append({0, 1, 2});
  //  table->append({1, 1, 2});
  //  table->append({0, 1, 2});
  //  table->append({1, 0, 2});
  //  table->append({2, 3, 2});
  //  table->append({2, 3, 2});
  //  table->append({4, 3, 2});
  const auto row_count = size_t{5'000'000};
  Segments segments;
  segments.emplace_back(make_int_segment<int32_t>(row_count, 0, 2000));
  segments.emplace_back(make_int_segment<int32_t>(row_count, 0, 2000));
  segments.emplace_back(make_int_segment<int32_t>(row_count, 0, 200));
  table->append_chunk(segments);

  return table;
}

std::shared_ptr<Table> make_string_table() {
  TableColumnDefinitions table_column_definitions;
  table_column_definitions.emplace_back("a", DataType::String);
  table_column_definitions.emplace_back("b", DataType::String);
  table_column_definitions.emplace_back("c", DataType::String);
  table_column_definitions.emplace_back("d", DataType::Int);
  table_column_definitions.emplace_back("e", DataType::Long);
  table_column_definitions.emplace_back("f", DataType::Int);

  const auto table = std::make_shared<Table>(table_column_definitions, TableType::Data);
//  table->append({"abcd", "bc", 3});
//  table->append({"aa", "bcd", 4});
//  table->append({"aa", "bcd", 4});
  const auto row_count = size_t{1'000'000};
  Segments segments;
  segments.emplace_back(make_date_segment(row_count, 0, 20));
  segments.emplace_back(make_date_segment(row_count, 0, 10));
  segments.emplace_back(make_date_segment(row_count, 0, 1));
  segments.emplace_back(make_int_segment<int32_t>(row_count, 0, 200));
  segments.emplace_back(make_int_segment<int64_t>(row_count, 0, 100));
  segments.emplace_back(make_int_segment<int32_t>(row_count, 0, 20));
  table->append_chunk(segments);

  return table;
}

std::shared_ptr<Table> make_lineitem_table() {
  TpchTableGenerator{0.1f}.generate_and_store();
  return StorageManager::get().get_table("lineitem");
}

int main() {
  const auto table = make_string_table();
  //  Print::print(table);

  const auto table_op = std::make_shared<TableWrapper>(table);
  table_op->execute();

  auto aggregates = std::vector<AggregateColumnDefinition>{};
  //aggregates.emplace_back(ColumnID{3}, AggregateFunction::Avg);

  auto group_by_column_ids = std::vector<ColumnID>{};
  group_by_column_ids.emplace_back(ColumnID{3});
  group_by_column_ids.emplace_back(ColumnID{4});

  {
    Timer t1;
    const auto aggregate_op2 = std::make_shared<AggregateHash>(table_op, aggregates, group_by_column_ids);
    aggregate_op2->execute();
    std::cout << "Hash: " << t1.lap_formatted() << std::endl;
    const auto aggregate_op = std::make_shared<AggregateHashSort>(table_op, aggregates, group_by_column_ids);
    aggregate_op->execute();
    std::cout << "HashSort: " << t1.lap_formatted() << std::endl;

    std::cout << "HashSort: " << aggregate_op->get_output()->row_count() << " row; " << aggregate_op->get_output()->chunk_count() << " chunks" << std::endl;
    std::cout << "Hash: " << aggregate_op2->get_output()->row_count() << " row; " << aggregate_op2->get_output()->chunk_count() << " chunks" << std::endl;

//    if (aggregate_op->get_output()->row_count() < 20) {
//      Print::print(aggregate_op->get_output(), PrintFlags::IgnoreChunkBoundaries);
//      std::cout << std::endl;
//      Print::print(aggregate_op2->get_output(), PrintFlags::IgnoreChunkBoundaries);
//    }
  }
//  {
//    Timer t1;
//    const auto aggregate_op2 = std::make_shared<AggregateHash>(table_op, aggregates, group_by_column_ids);
//    aggregate_op2->execute();
//    std::cout << "Hash: " << t1.lap_formatted() << std::endl;
//    const auto aggregate_op = std::make_shared<AggregateHashSort>(table_op, aggregates, group_by_column_ids);
//    aggregate_op->execute();
//    std::cout << "HashSort: " << t1.lap_formatted() << std::endl;
//  }

  return 0;
}
