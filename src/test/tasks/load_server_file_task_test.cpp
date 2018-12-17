
#include "base_test.hpp"
#include "gtest/gtest.h"

#include "tasks/server/load_server_file_task.hpp"

namespace opossum {

class LoadServerFileTaskTest : public BaseTest {};

TEST_F(LoadServerFileTaskTest, LoadsDifferentFileTypes) {
  auto tbl_task = std::make_shared<LoadServerFileTask>("src/test/tables/int_float.tbl", "int_float_tbl");
  tbl_task->execute();
  EXPECT_TRUE(StorageManager::get().has_table("int_float_tbl"));

  auto csv_task = std::make_shared<LoadServerFileTask>("src/test/tables/int_float.csv", "int_float_csv");
  csv_task->execute();
  EXPECT_TRUE(StorageManager::get().has_table("int_float_csv"));

  auto bin_task = std::make_shared<LoadServerFileTask>("src/test/tables/int_float.bin", "int_float_bin");
  bin_task->execute();
  EXPECT_TRUE(StorageManager::get().has_table("int_float_bin"));

  //  auto fail_task = std::make_shared<LoadServerFileTask>("unsupport.ed", "unsupported");
  //  EXPECT_THROW(fail_task->execute(), std::exception);
}

}  // namespace opossum
