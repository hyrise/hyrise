#include <iostream>
#include <memory>

#include "hyrise.hpp"
#include "operators/insert.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  const auto world = pmr_string{"world"};
  std::cout << "Hello " << world << "!\n";

  const auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data,
                                             Chunk::DEFAULT_SIZE, UseMvcc::Yes);
  Hyrise::get().storage_manager.add_table("table_a", table);
  //Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  const auto values_to_insert =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
  values_to_insert->append({int32_t{123}});
  values_to_insert->append({int32_t{456}});

  for (auto init_insert_id = uint8_t{0}; init_insert_id < 17; ++init_insert_id) {
    SQLPipelineBuilder{"INSERT INTO table_a VALUES( " + std::to_string(init_insert_id) + ");"}
        .create_pipeline()
        .get_result_table();
  }

  // We observed long runtimes in Debug builds, especially with UBSan enabled. Thus, we reduce the load a bit in this
  // case.
  const auto insert_thread_count = uint32_t{20};
  const auto watch_thread_count = uint32_t{50};
  auto insert_threads = std::vector<std::thread>{};
  insert_threads.reserve(insert_thread_count);

  const auto loop_count = uint32_t{100'000};

  for (auto thread_id = uint32_t{0}; thread_id < insert_thread_count; ++thread_id) {
    insert_threads.emplace_back([&]() {
      auto my_loop = uint32_t{0};
      while (my_loop < loop_count) {
        ++my_loop;
        const auto table_wrapper = std::make_shared<TableWrapper>(values_to_insert);
        const auto insert = std::make_shared<Insert>("table_a", table_wrapper);

        const auto do_commit = false;
        const auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
        insert->set_transaction_context(transaction_context);
        table_wrapper->execute();
        insert->execute();
        Assert(!insert->execute_failed(), "Should succeed.");
        if (do_commit) {
          transaction_context->commit();
        } else {
          transaction_context->rollback(RollbackReason::User);
        }
      }
    });
  }

  auto watch_threads = std::vector<std::thread>{};
  watch_threads.reserve(watch_thread_count);

  for (auto thread_id = uint32_t{0}; thread_id < watch_thread_count; ++thread_id) {
    watch_threads.emplace_back([&]() {
      auto my_loop = uint32_t{0};
      while (my_loop < loop_count) {
        ++my_loop;
        {
          const auto [status, result_table] =
              SQLPipelineBuilder{"SELECT count(*) from table_a;"}.create_pipeline().get_result_table();
          Assert(status == SQLPipelineStatus::Success, "Expected success.");
          const auto visible_row_count = result_table->get_value<int64_t>(ColumnID{0}, 0);
          Assert(visible_row_count, "Must not be nullable.");
          Assert(*visible_row_count == 17, "Invisible rows are visible: " + std::to_string(*visible_row_count));
        }

        {
          const auto [status, result_table] =
              SQLPipelineBuilder{"SELECT max(a) from table_a;"}.create_pipeline().get_result_table();
          Assert(status == SQLPipelineStatus::Success, "Expected success.");
          const auto max_value = result_table->get_value<int32_t>(ColumnID{0}, 0);
          Assert(max_value, "Must not be nullable.");
          Assert(*max_value == 16, "Invisible rows are visible: " + std::to_string(*max_value));
        }
      }
    });
  }

  for (auto& thread : insert_threads) {
    thread.join();
  }

  for (auto& thread : watch_threads) {
    thread.join();
  }

  return 0;
}
