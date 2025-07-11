#include "base_test.hpp"
#include "concurrency/transaction_manager.hpp"
#include "hyrise.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/validate.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/plugin_test_utils.hpp"

namespace hyrise {

class HyriseTest : public BaseTest {
 protected:
  // This wrapper method is needed to access the plugins vector since it is a private member of PluginManager
  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() {
    auto& pm = Hyrise::get().plugin_manager;

    return pm._plugins;
  }
};

TEST_F(HyriseTest, GetAndResetHyrise) {
  auto& hyrise = Hyrise::get();

  EXPECT_EQ(get_plugins().size(), 0);
  hyrise.plugin_manager.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  EXPECT_EQ(get_plugins().size(), 1);

  const auto table_name = "test_table";

  EXPECT_FALSE(hyrise.storage_manager.has_table(table_name));
  const auto table = std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, false}}, TableType::Data);
  hyrise.storage_manager.add_table(table_name, table);
  EXPECT_TRUE(hyrise.storage_manager.has_table(table_name));

  EXPECT_EQ(hyrise.transaction_manager.last_commit_id(), CommitID{1});

  // We need to do some honest work so that the commit id is actually incremented
  const auto get_table = std::make_shared<GetTable>(table_name);
  const auto validate = std::make_shared<Validate>(get_table);
  const auto delete_op = std::make_shared<Delete>(validate);
  const auto transaction_context = hyrise.transaction_manager.new_transaction_context(AutoCommit::No);
  delete_op->set_transaction_context_recursively(transaction_context);
  get_table->execute();
  validate->execute();
  delete_op->execute();
  transaction_context->commit();

  EXPECT_EQ(hyrise.transaction_manager.last_commit_id(), CommitID{2});

  Hyrise::reset();

  EXPECT_EQ(get_plugins().size(), 0);
  EXPECT_FALSE(hyrise.storage_manager.has_table(table_name));
  EXPECT_EQ(hyrise.transaction_manager.last_commit_id(), CommitID{1});
}

TEST_F(HyriseTest, ChangingSchedulers) {
  auto counter = std::atomic<uint32_t>{0};
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto task1 = std::make_shared<JobTask>([&]() {
    ++counter;
  });
  task1->schedule();

  // Implicitely tests that changing the scheduler calls `finish()` on the old scheduler. Thus, we explicitely do not
  // wait for task1 here.
  Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
  const auto task2 = std::make_shared<JobTask>([&]() {
    ++counter;
  });
  task2->schedule();
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto task3 = std::make_shared<JobTask>([&]() {
    ++counter;
  });
  task3->schedule();
  Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());

  EXPECT_EQ(counter, 3);
}

}  // namespace hyrise
