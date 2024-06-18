#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "operators/print.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "tpcc/constants.hpp"
#include "tpcc/procedures/tpcc_payment.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

void payments() {
  std::unordered_map<std::string, BenchmarkTableInfo> tables;
  constexpr auto NUM_WAREHOUSES = 10;
  const auto thread_count = std::thread::hardware_concurrency();
  constexpr auto ITERATIONS_PER_THREAD = uint32_t{50'000};

  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  auto table_generator = TPCCTableGenerator{NUM_WAREHOUSES, benchmark_config};
  tables = table_generator.generate();

  for (const auto& [table_name, table_info] : tables) {
    // Copy the data into a new table in order to isolate tests
    const auto generated_table = table_info.table;
    auto isolated_table =
        std::make_shared<Table>(generated_table->column_definitions(), TableType::Data, std::nullopt, UseMvcc::Yes);
    Hyrise::get().storage_manager.add_table(table_name, isolated_table);

    auto table_wrapper = std::make_shared<TableWrapper>(generated_table);
    table_wrapper->execute();
    auto insert = std::make_shared<Insert>(table_name, table_wrapper);
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    insert->set_transaction_context(transaction_context);
    insert->execute();
    transaction_context->commit();
  }

  auto successful_runs = std::atomic_uint32_t{0};
  auto failed_runs = std::atomic_uint32_t{0};

  auto threads = std::vector<std::thread>{};
  threads.reserve(thread_count);

  for (auto thread_id = uint32_t{0}; thread_id < thread_count; ++thread_id) {
    // for (auto thread_id = uint32_t{0}; thread_id < 1; ++thread_id) {
    threads.emplace_back([&]() {
      for (auto iteration = uint32_t{0}; iteration < ITERATIONS_PER_THREAD; ++iteration) {
        auto sql_executor = BenchmarkSQLExecutor{nullptr, std::nullopt};
        auto payment = TPCCPayment{NUM_WAREHOUSES, sql_executor};
        const auto return_value = payment.execute();
        // ASSERT_TRUE(payment.execute());

        successful_runs += return_value;
        failed_runs += !return_value;
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  std::cout << successful_runs.load() << " & " << failed_runs.load() << std::endl;

  Hyrise::get().scheduler()->finish();
}

void single_updates(const int32_t warehouse_count = 1) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Int, false}};
  const auto table = std::make_shared<Table>(definitions, TableType::Data, ChunkOffset{3}, UseMvcc::Yes);
  for (auto warehouse_id = int32_t{0}; warehouse_id < warehouse_count; ++warehouse_id) {
    table->append({warehouse_id, int32_t{0}});
  }

  Hyrise::get().storage_manager.add_table("table_a", table);

  const auto column_a = hyrise::expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "a");
  const auto column_b = hyrise::expression_functional::pqp_column_(ColumnID{1}, DataType::Int, false, "b");
  const auto filter_predicate = hyrise::expression_functional::equals_(column_a, 1);
  const auto update_expressions =
      hyrise::expression_functional::expression_vector(column_a, hyrise::expression_functional::add_(column_b, 1));

  const auto updated = std::make_shared<Table>(definitions, TableType::Data);

  const auto update_count = 3'000;
  const auto thread_count = std::thread::hardware_concurrency();
  auto threads = std::vector<std::thread>{};
  threads.reserve(thread_count);

  auto successful_updates = std::atomic<uint64_t>{0};
  auto failed_updates = std::atomic<uint64_t>{0};

  const auto select_sql = std::string{"SELECT a, b FROM table_a WHERE a = ?;"};
  const auto update_sql = std::string{"UPDATE table_a SET b = b + 1 WHERE a = ?;"};

  for (auto thread_id = uint32_t{0}; thread_id < thread_count; ++thread_id) {
    threads.emplace_back([&]() {
      auto random_device = std::random_device{};
      auto select_distribution = std::uniform_int_distribution<int8_t>{0, 9};
      auto warehouse_distribution = std::uniform_int_distribution<int32_t>{0, warehouse_count - 1};

      for (auto iteration = uint32_t{0}; iteration < update_count; ++iteration) {
        const auto random_value = select_distribution(random_device);
        const auto select_only = random_value < 5;

        const auto picked_warehouse_id = warehouse_distribution(random_device);

        if (select_only) {
          auto select_sql_exec = select_sql;
          select_sql_exec.replace(select_sql_exec.find("?"), 1, std::to_string(picked_warehouse_id));
          auto select_pipeline = SQLPipelineBuilder{select_sql_exec}.create_pipeline();
          const auto [status, result_table] = select_pipeline.get_result_table();
          Assert(status == SQLPipelineStatus::Success, "no success");
          Assert(result_table->row_count() == 1, "ROW COUNT!");
        } else {
          auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);

          auto select_sql_exec = select_sql;
          auto update_sql_exec = update_sql;
          select_sql_exec.replace(select_sql_exec.find("?"), 1, std::to_string(picked_warehouse_id));
          update_sql_exec.replace(update_sql_exec.find("?"), 1, std::to_string(picked_warehouse_id));
          const auto combined_sql = select_sql_exec + " " + update_sql_exec;

          auto select_pipeline =
              SQLPipelineBuilder{combined_sql}.with_transaction_context(transaction_context).create_pipeline();
          const auto [status, result_tables] = select_pipeline.get_result_tables();

          if (status == SQLPipelineStatus::Success) {
            if (random_value == 7) {
              std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            transaction_context->commit();
            Assert(result_tables.size() == 2, "Table count");
            Assert(result_tables[0]->row_count() == 1, "ROW COUJNT!!!");

            ++successful_updates;
          } else {
            ++failed_updates;
          }
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  Assert(successful_updates.load() > 2, "Nooo");
  std::cout << successful_updates.load() << " & " << failed_updates.load() << std::endl;

  auto select_pipeline = SQLPipelineBuilder{"SELECT a, b FROM table_a WHERE a = 1;"}.create_pipeline();
  const auto [status, result_table] = select_pipeline.get_result_table();
  Assert(status == SQLPipelineStatus::Success, "Hmpf");
  Assert(result_table->row_count() == 1, "Hmpf");

  auto select_pipeline_all = SQLPipelineBuilder{"SELECT * FROM table_a;"}.create_pipeline();
  const auto [status_all, result_table_all] = select_pipeline_all.get_result_table();
  Assert(status_all == SQLPipelineStatus::Success, "Hmpf");
  Print::print(result_table_all);

  Hyrise::get().scheduler()->finish();
}

int main() {
  // payments();
  single_updates(10);
  return 0;
}
