#include "base_test.hpp"

#include "tasks/server/load_server_file_task.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class LoadServerFileTaskTest : public BaseTest {
 public:
  void SetUp() override { int_float_expected = load_table("resources/test_data/tbl/int_float.tbl"); }

  std::shared_ptr<Table> int_float_expected;
};

TEST_F(LoadServerFileTaskTest, LoadsDifferentFileTypes) {
  auto tbl_task = std::make_shared<LoadServerFileTask>("resources/test_data/tbl/int_float.tbl", "int_float_tbl");
  tbl_task->execute();
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("int_float_tbl"), int_float_expected);

  auto csv_task = std::make_shared<LoadServerFileTask>("resources/test_data/csv/int_float.csv", "int_float_csv");
  csv_task->execute();
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("int_float_csv"), int_float_expected);

  auto bin_task = std::make_shared<LoadServerFileTask>("resources/test_data/bin/int_float.bin", "int_float_bin");
  bin_task->execute();
  EXPECT_TABLE_EQ_ORDERED(Hyrise::get().storage_manager.get_table("int_float_bin"), int_float_expected);

  auto fail_task = std::make_shared<LoadServerFileTask>("unsupport.ed", "unsupported");
  auto future = fail_task->get_future();
  fail_task->execute();
  EXPECT_THROW(future.get(), std::exception);
}

}  // namespace opossum
