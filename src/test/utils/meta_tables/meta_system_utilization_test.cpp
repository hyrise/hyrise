#include "base_test.hpp"

#include "utils/meta_tables/meta_system_utilization_table.hpp"

namespace opossum {

class MetaSystemUtilizationTest : public BaseTest {
 protected:
  static std::shared_ptr<Table> system_utilization_idle_1;
  static std::shared_ptr<Table> system_utilization_idle_2;
  static std::shared_ptr<Table> system_utilization_load_1;
  static std::shared_ptr<Table> system_utilization_load_2;

  static size_t memory_estimation;

  static void SetUpTestSuite() {
    const auto meta_system_utilization_table = std::make_shared<MetaSystemUtilizationTable>();

    // idle measurement
    system_utilization_idle_1 = generate_meta_table(meta_system_utilization_table);
    usleep(10);
    system_utilization_idle_2 = generate_meta_table(meta_system_utilization_table);

    const auto dummy_table = create_dummy_table();
    auto dummy_threads = create_dummy_threads();

    // load measurement
    system_utilization_load_1 = generate_meta_table(meta_system_utilization_table);
    usleep(10);
    system_utilization_load_2 = generate_meta_table(meta_system_utilization_table);

    for (uint32_t thread_index = 0; thread_index < 100; ++thread_index) {
      dummy_threads[thread_index].join();
    }

    memory_estimation = dummy_table->memory_usage(MemoryUsageCalculationMode::Full);
  }

  static const std::shared_ptr<Table> generate_meta_table(const std::shared_ptr<AbstractMetaTable>& table) {
    return table->_generate();
  }

  static const std::shared_ptr<Table> create_dummy_table() {
    const auto dummy_table =
        std::make_shared<Table>(TableColumnDefinitions{{"dummy_column", DataType::Long, false}}, TableType::Data, 5);
    // Creates some dummy data
    const auto dummy_table_columns = 1024 * 1024 / 64;
    for (uint64_t column = 0; column < dummy_table_columns; ++column) {
      dummy_table->append({static_cast<int64_t>(column)});
    }
    return dummy_table;
  }

  static std::vector<std::thread> create_dummy_threads() {
    std::vector<std::thread> threads(100);
    for (uint32_t thread_index = 0; thread_index < 100; ++thread_index) {
      threads[thread_index] = std::thread(dummy_load);
    }
    return threads;
  }

  static void dummy_load() {
    // busy waiting
    for (uint32_t i = 0; i < 10'000'000; ++i) {
      continue;
    }
  }
};

std::shared_ptr<Table> MetaSystemUtilizationTest::system_utilization_idle_1;
std::shared_ptr<Table> MetaSystemUtilizationTest::system_utilization_idle_2;
std::shared_ptr<Table> MetaSystemUtilizationTest::system_utilization_load_1;
std::shared_ptr<Table> MetaSystemUtilizationTest::system_utilization_load_2;

size_t MetaSystemUtilizationTest::memory_estimation;

TEST_F(MetaSystemUtilizationTest, ProcessMemoryUsage) {
  const auto virtual_memory_column_id = system_utilization_idle_1->column_id_by_name("process_virtual_memory");
  const auto rss_column_id = system_utilization_idle_1->column_id_by_name("process_physical_memory_RSS");

  const auto virtual_memory_idle = boost::get<int64_t>(system_utilization_idle_1->get_row(0)[virtual_memory_column_id]);
  const auto virtual_memory_load = boost::get<int64_t>(system_utilization_load_1->get_row(0)[virtual_memory_column_id]);
  const auto virtual_memory_diff = virtual_memory_load - virtual_memory_idle;

  const auto rss_idle = boost::get<int64_t>(system_utilization_idle_1->get_row(0)[rss_column_id]);
  const auto rss_load = boost::get<int64_t>(system_utilization_load_1->get_row(0)[rss_column_id]);
  const auto rss_diff = rss_load - rss_idle;

  EXPECT_GE(static_cast<size_t>(virtual_memory_diff), memory_estimation);
  // This should be true if enough memory is available and no other processes allocate memory concurrently
  // EXPECT_GE(static_cast<size_t>(rss_diff), memory_estimation);
}

// This test may fail if it is executed in parallel with other memory intensive processes.
TEST_F(MetaSystemUtilizationTest, SystemMemoryUsage) {
  // Since the OS doesn't immediately take back freed memory, this test might not work repeatedly within one process-
  static bool first_run = true;
  if (first_run) {
    first_run = false;
  } else {
    GTEST_SKIP();
  }

  const auto free_memory_column_id = system_utilization_idle_1->column_id_by_name("system_memory_free");
  const auto available_memory_column_id = system_utilization_idle_1->column_id_by_name("system_memory_available");

  const auto free_memory_idle = boost::get<int64_t>(system_utilization_idle_1->get_row(0)[free_memory_column_id]);
  const auto free_memory_load = boost::get<int64_t>(system_utilization_load_1->get_row(0)[free_memory_column_id]);
  const auto free_memory_diff = free_memory_idle - free_memory_load;

  const auto available_memory_idle =
      boost::get<int64_t>(system_utilization_idle_1->get_row(0)[available_memory_column_id]);
  const auto available_memory_load =
      boost::get<int64_t>(system_utilization_load_1->get_row(0)[available_memory_column_id]);
  const auto available_memory_diff = available_memory_idle - available_memory_load;

  EXPECT_GE(static_cast<size_t>(free_memory_diff), memory_estimation);
  EXPECT_GE(static_cast<size_t>(available_memory_diff), memory_estimation);
}

TEST_F(MetaSystemUtilizationTest, ProcessCPUUsage) {
  const auto total_time_column_id = system_utilization_idle_1->column_id_by_name("total_time");
  const auto cpu_process_time_column_id = system_utilization_idle_1->column_id_by_name("cpu_process_time");

  const auto total_time_idle_1 = boost::get<int64_t>(system_utilization_idle_1->get_row(0)[total_time_column_id]);
  const auto total_time_idle_2 = boost::get<int64_t>(system_utilization_idle_2->get_row(0)[total_time_column_id]);
  const auto total_time_idle_diff = total_time_idle_2 - total_time_idle_1;
  EXPECT_GT(total_time_idle_diff, 0);

  const auto cpu_process_time_idle_1 =
      boost::get<int64_t>(system_utilization_idle_1->get_row(0)[cpu_process_time_column_id]);
  const auto cpu_process_time_idle_2 =
      boost::get<int64_t>(system_utilization_idle_2->get_row(0)[cpu_process_time_column_id]);
  const auto cpu_process_time_idle_diff = cpu_process_time_idle_2 - cpu_process_time_idle_1;
  EXPECT_GT(cpu_process_time_idle_diff, 0);

  const auto process_cpu_idle_utilization =
      static_cast<float>(cpu_process_time_idle_diff) / static_cast<float>(total_time_idle_diff);

  const auto total_time_load_1 = boost::get<int64_t>(system_utilization_load_1->get_row(0)[total_time_column_id]);
  const auto total_time_load_2 = boost::get<int64_t>(system_utilization_load_2->get_row(0)[total_time_column_id]);
  const auto total_time_load_diff = total_time_load_2 - total_time_load_1;
  EXPECT_GT(total_time_load_diff, 0);

  const auto cpu_process_time_load_1 =
      boost::get<int64_t>(system_utilization_load_1->get_row(0)[cpu_process_time_column_id]);
  const auto cpu_process_time_load_2 =
      boost::get<int64_t>(system_utilization_load_2->get_row(0)[cpu_process_time_column_id]);
  const auto cpu_process_time_load_diff = cpu_process_time_load_2 - cpu_process_time_load_1;
  EXPECT_GT(cpu_process_time_load_diff, 0);

  const auto process_cpu_load_utilization =
      static_cast<float>(cpu_process_time_load_diff) / static_cast<float>(total_time_load_diff);

  EXPECT_GE(process_cpu_load_utilization, process_cpu_idle_utilization);
}

// This test may fail if it is executed in parallel with other CPU intensive processes.
TEST_F(MetaSystemUtilizationTest, SystemCPUUsage) {
  const auto total_time_column_id = system_utilization_idle_1->column_id_by_name("total_time");
  const auto cpu_system_time_column_id = system_utilization_idle_1->column_id_by_name("cpu_system_time");

  const auto total_time_idle_1 = boost::get<int64_t>(system_utilization_idle_1->get_row(0)[total_time_column_id]);
  const auto total_time_idle_2 = boost::get<int64_t>(system_utilization_idle_2->get_row(0)[total_time_column_id]);
  const auto total_time_idle_diff = total_time_idle_2 - total_time_idle_1;
  EXPECT_GT(total_time_idle_diff, 0);

  const auto cpu_system_time_idle_1 =
      boost::get<int64_t>(system_utilization_idle_1->get_row(0)[cpu_system_time_column_id]);
  const auto cpu_system_time_idle_2 =
      boost::get<int64_t>(system_utilization_idle_2->get_row(0)[cpu_system_time_column_id]);
  const auto cpu_system_time_idle_diff = cpu_system_time_idle_2 - cpu_system_time_idle_1;
  EXPECT_GT(cpu_system_time_idle_diff, 0);

  const auto system_cpu_idle_utilization =
      static_cast<float>(cpu_system_time_idle_diff) / static_cast<float>(total_time_idle_diff);

  const auto total_time_load_1 = boost::get<int64_t>(system_utilization_load_1->get_row(0)[total_time_column_id]);
  const auto total_time_load_2 = boost::get<int64_t>(system_utilization_load_2->get_row(0)[total_time_column_id]);
  const auto total_time_load_diff = total_time_load_2 - total_time_load_1;
  EXPECT_GT(total_time_load_diff, 0);

  const auto cpu_system_time_load_1 =
      boost::get<int64_t>(system_utilization_load_1->get_row(0)[cpu_system_time_column_id]);
  const auto cpu_system_time_load_2 =
      boost::get<int64_t>(system_utilization_load_2->get_row(0)[cpu_system_time_column_id]);
  const auto cpu_system_time_load_diff = cpu_system_time_load_2 - cpu_system_time_load_1;
  EXPECT_GT(cpu_system_time_load_diff, 0);

  const auto system_cpu_load_utilization =
      static_cast<float>(cpu_system_time_load_diff) / static_cast<float>(total_time_load_diff);

  EXPECT_GE(system_cpu_load_utilization, system_cpu_idle_utilization);
}

}  // namespace opossum
