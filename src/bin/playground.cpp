#include <iostream>

#include "concurrency/transaction_manager.hpp"
#include "concurrency/transaction_context.hpp"
#include "operators/import_csv.hpp"
#include "operators/insert.hpp"
#include "operators/print.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "sql/sql_pipeline.hpp"

#include "types.hpp"

using namespace opossum;  // NOLINT

std::string random_string(int length) {
    static std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    std::string result;
    result.resize(length);

    for (int i = 0; i < length; i++)
        result[i] = charset[rand() % charset.length()];

    return result;
}

void insert_benchmark() {
  auto& storage_manager = StorageManager::get();
  auto& transaction_manager = TransactionManager::get();

  // setup partitioned and unpartitioned table
  auto table_partitioned = std::make_shared<Table>();
  HashFunction hash_function;
  table_partitioned->create_hash_partitioning(ColumnID{0}, hash_function, 10);
  table_partitioned->add_column("one", DataType::String);
  table_partitioned->add_column("two", DataType::Int);
  table_partitioned->add_column("three", DataType::Float);

  auto table_unpartitioned = std::make_shared<Table>();
  table_unpartitioned->add_column("one", DataType::String);
  table_unpartitioned->add_column("two", DataType::Int);
  table_unpartitioned->add_column("three", DataType::Float);

  // add tables to StorageManager
  storage_manager.add_table("PartitionedTable", table_partitioned);
  storage_manager.add_table("UnpartitionedTable", table_unpartitioned);

  // create table with random data
  auto table_insert = std::make_shared<Table>();
  table_insert->add_column("one", DataType::String);
  table_insert->add_column("two", DataType::Int);
  table_insert->add_column("three", DataType::Float);
  auto table_insert_wrapped = std::make_shared<TableWrapper>(table_insert);
  for (size_t count = 0; count < 1000000; ++count) {
    std::string value_s = random_string(32);
    int value_i = count;
    float value_f = static_cast<float>(count) / (std::rand() + 2);
    table_insert->append({std::move(value_s), std::move(value_i), std::move(value_f)});
  }
  table_insert_wrapped->execute();

  // benchmark for partitioned table
  auto context_partitioned = transaction_manager.new_transaction_context();
  auto op_insert_partitioned = std::make_shared<Insert>("PartitionedTable", table_insert_wrapped);
  op_insert_partitioned->set_transaction_context(context_partitioned);
  const auto time_start_partitioned = std::chrono::steady_clock::now();
  op_insert_partitioned->execute();
  const auto time_end_partitioned = std::chrono::steady_clock::now();
  const auto duration_partitioned = std::chrono::duration_cast<std::chrono::microseconds>(time_end_partitioned - time_start_partitioned).count();
  context_partitioned->commit();

  // benchmark for unpartitioned table
  auto context_unpartitioned = transaction_manager.new_transaction_context();
  auto op_insert_unpartitioned = std::make_shared<Insert>("UnpartitionedTable", table_insert_wrapped);
  op_insert_unpartitioned->set_transaction_context(context_unpartitioned);
  const auto time_start_unpartitioned = std::chrono::steady_clock::now();
  op_insert_unpartitioned->execute();
  const auto time_end_unpartitioned = std::chrono::steady_clock::now();
  const auto duration_unpartitioned = std::chrono::duration_cast<std::chrono::microseconds>(time_end_unpartitioned - time_start_unpartitioned).count();
  context_unpartitioned->commit();
  
  // print times
  std::cout << "=== Insert Benchmark Results" << std::endl;
  std::cout << "Partitioned:   " << duration_partitioned << " us" << std::endl;
  std::cout << "Unpartitioned: " << duration_unpartitioned << " us" << std::endl;
}

void select_benchmark() {
  auto& transaction_manager = TransactionManager::get();

  // import big dataset (twice)
  const std::string path_to_csv = "";
  auto op_import_partitioned = std::make_shared<ImportCsv>(path_to_csv, "ImportedPartitionedTable");
  op_import_partitioned->execute();
  auto op_import_unpartitioned = std::make_shared<ImportCsv>(path_to_csv, "ImportedUnartitionedTable");
  op_import_unpartitioned->execute();
  
  // TODO(anyone): add operator for partitioning table
  // apply partitioning on one table
  // auto partition_schema = std::make_shared<RangePartitionSchema>(ColumnID{2}, {200, 300, 500});
  // auto op_partition_table = std::make_shared<PartitionTable>("ImportedPartitionedTable", partition_schema)
  // op_partition_table->execute();

  // benchmark for partitioned table
  SQLPipeline sql_partitioned{"SELECT * FROM ImportedPartitionedTable WHERE x > 310 AND x < 450", transaction_manager.new_transaction_context()};
  const auto time_start_partitioned = std::chrono::steady_clock::now();
  sql_partitioned.get_result_table();
  const auto time_end_partitioned = std::chrono::steady_clock::now();
  const auto duration_partitioned = std::chrono::duration_cast<std::chrono::microseconds>(time_end_partitioned - time_start_partitioned).count();

  // benchmark for unpartitioned table
  SQLPipeline sql_unpartitioned{"SELECT * FROM ImportedUnpartitionedTable WHERE x > 310 AND x < 450", transaction_manager.new_transaction_context()};
  const auto time_start_unpartitioned = std::chrono::steady_clock::now();
  sql_unpartitioned.get_result_table();
  const auto time_end_unpartitioned = std::chrono::steady_clock::now();
  const auto duration_unpartitioned = std::chrono::duration_cast<std::chrono::microseconds>(time_end_unpartitioned - time_start_unpartitioned).count();

  // print times
  std::cout << "=== Select Benchmark Results" << std::endl;
  std::cout << "Partitioned:   " << duration_partitioned << " us" << std::endl;
  std::cout << "Unpartitioned: " << duration_unpartitioned << " us" << std::endl;
}

int main() {
  std::srand(std::time(NULL));

  // insert_benchmark();
  // select_benchmark();

  auto table = std::make_shared<Table>(10);
  table->create_range_partitioning(ColumnID{1}, {100000000, 1000000000});
  table->add_column("country", DataType::String);
  table->add_column("population", DataType::Int, true);
  table->append({"China", 1388720000});
  table->append({"India", 1326720000});
  table->append({"United States", 326474000});
  table->append({"Germany", 82521653});

  auto table_wrapper = std::make_shared<TableWrapper>(table);
  table_wrapper->execute();

  auto print_partitioned_operator = std::make_shared<Print>(table_wrapper, std::cout);
  std::cout << "### Partitioned table" << std::endl;
  print_partitioned_operator->execute();
  std::cout << std::endl << std::endl;

  auto sort_operator = std::make_shared<Sort>(table_wrapper, ColumnID{0});
  sort_operator->execute();

  auto print_operator = std::make_shared<Print>(sort_operator, std::cout);
  std::cout << "### Sorted table" << std::endl;
  print_operator->execute();

  return 0;
}
