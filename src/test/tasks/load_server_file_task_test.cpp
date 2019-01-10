
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "tasks/server/load_server_file_task.hpp"

namespace opossum {

class LoadServerFileTaskTest : public BaseTest {};

TEST_F(LoadServerFileTaskTest, LoadsDifferentFileTypes) {
  auto tbl_task = std::make_shared<LoadServerFileTask>("resources/test_data/tbl/int_float.tbl", "int_float_tbl");
  tbl_task->execute();
  EXPECT_TRUE(StorageManager::get().has_table("int_float_tbl"));

  auto csv_task = std::make_shared<LoadServerFileTask>("resources/test_data/csv/int_float.csv", "int_float_csv");
  csv_task->execute();
  EXPECT_TRUE(StorageManager::get().has_table("int_float_csv"));

  auto bin_task = std::make_shared<LoadServerFileTask>("resources/test_data/bin/int_float.bin", "int_float_bin");
  bin_task->execute();
  EXPECT_TRUE(StorageManager::get().has_table("int_float_bin"));

  auto fail_task = std::make_shared<LoadServerFileTask>("unsupport.ed", "unsupported");
  auto future = fail_task->get_future();
  fail_task->execute();
  EXPECT_THROW(future.get(), std::exception);
}

}  // namespace opossum
