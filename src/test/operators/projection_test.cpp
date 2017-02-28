#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {

class OperatorsProjectionTest : public BaseTest {
 protected:
  void SetUp() override {
    std::shared_ptr<Table> test_table = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", std::move(test_table));
    _gt = std::make_shared<GetTable>("table_a");

    _gt->execute();
  }

  std::shared_ptr<GetTable> _gt;
};

TEST_F(OperatorsProjectionTest, SingleColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  std::vector<std::string> column_filter = {"a"};
  auto projection = std::make_shared<Projection>(_gt, column_filter);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, DoubleProject) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 3);

  std::vector<std::string> column_filter = {"a"};
  auto projection1 = std::make_shared<Projection>(_gt, column_filter);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(projection1, column_filter);
  projection2->execute();

  EXPECT_TABLE_EQ(projection2->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, AllColumns) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/float_int.tbl", 2);

  std::vector<std::string> column_filter = {"b", "a"};
  auto projection = std::make_shared<Projection>(_gt, column_filter);
  projection->execute();

  EXPECT_TABLE_EQ(projection->get_output(), expected_result);
}

TEST_F(OperatorsProjectionTest, ValueColumnCount) {
  std::vector<std::string> columns = {"a", "b"};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)3);

  columns = {"b"};
  auto projection_2 = std::make_shared<opossum::Projection>(_gt, columns);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)3);

  columns = {"a"};
  auto projection_3 = std::make_shared<opossum::Projection>(_gt, columns);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)3);
}

// TODO(anyone): refactor test
TEST_F(OperatorsProjectionTest, ReferenceColumnCount) {
  auto scan = std::make_shared<opossum::TableScan>(_gt, "a", "=", 1234);
  scan->execute();

  std::vector<std::string> columns = {"a", "b"};
  auto projection_1 = std::make_shared<opossum::Projection>(scan, columns);
  projection_1->execute();
  EXPECT_EQ(projection_1->get_output()->col_count(), (u_int)2);
  EXPECT_EQ(projection_1->get_output()->row_count(), (u_int)1);

  columns = {"a"};
  auto projection_2 = std::make_shared<opossum::Projection>(scan, columns);
  projection_2->execute();
  EXPECT_EQ(projection_2->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_2->get_output()->row_count(), (u_int)1);

  columns = {"b"};
  auto projection_3 = std::make_shared<opossum::Projection>(scan, columns);
  projection_3->execute();
  EXPECT_EQ(projection_3->get_output()->col_count(), (u_int)1);
  EXPECT_EQ(projection_3->get_output()->row_count(), (u_int)1);
}

TEST_F(OperatorsProjectionTest, NumInputTables) {
  std::vector<std::string> columns = {"a", "b"};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);

  EXPECT_EQ(projection_1->num_in_tables(), 1);
}

TEST_F(OperatorsProjectionTest, NumOutputTables) {
  std::vector<std::string> columns = {"a", "b"};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);

  EXPECT_EQ(projection_1->num_out_tables(), 1);
}

TEST_F(OperatorsProjectionTest, OperatorName) {
  std::vector<std::string> columns = {"a", "b"};
  auto projection_1 = std::make_shared<opossum::Projection>(_gt, columns);

  EXPECT_EQ(projection_1->name(), "Projection");
}

}  // namespace opossum
