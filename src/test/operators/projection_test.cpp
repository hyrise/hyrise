#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
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
  }

  std::shared_ptr<GetTable> _gt;
};

TEST_F(OperatorsProjectionTest, SingleColumn) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 1);

  std::vector<std::string> column_filter = {"a"};
  auto projection = std::make_shared<Projection>(_gt, column_filter);
  projection->execute();

  EXPECT_TABLE_EQ(*(projection->get_output()), *expected_result);
}

TEST_F(OperatorsProjectionTest, DoubleProject) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int.tbl", 3);

  std::vector<std::string> column_filter = {"a"};
  auto projection1 = std::make_shared<Projection>(_gt, column_filter);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(projection1, column_filter);
  projection2->execute();

  EXPECT_TABLE_EQ(*(projection2->get_output()), *expected_result);
}

TEST_F(OperatorsProjectionTest, AllColumns) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float.tbl", 2);

  std::vector<std::string> column_filter = {"a", "b"};
  auto projection = std::make_shared<Projection>(_gt, column_filter);
  projection->execute();

  EXPECT_TABLE_EQ(*(projection->get_output()), *expected_result);
}

}  // namespace opossum