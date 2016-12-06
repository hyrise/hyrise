#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/difference.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {
class OperatorsDifferenceTest : public BaseTest {
 protected:
  virtual void SetUp() {
    std::shared_ptr<Table> test_table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("difference_test_table_a", std::move(test_table_a));
    _gt_a = std::make_shared<GetTable>("difference_test_table_a");

    std::shared_ptr<Table> test_table_b = load_table("src/test/tables/int_float3.tbl", 2);
    StorageManager::get().add_table("difference_test_table_b", std::move(test_table_b));
    _gt_b = std::make_shared<GetTable>("difference_test_table_b");
  }

  std::shared_ptr<GetTable> _gt_a;
  std::shared_ptr<GetTable> _gt_b;
};

TEST_F(OperatorsDifferenceTest, DifferenceOnValueTables) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 2);

  auto difference = std::make_shared<Difference>(_gt_a, _gt_b);
  difference->execute();

  EXPECT_TABLE_EQ(difference->get_output(), expected_result);
}

TEST_F(OperatorsDifferenceTest, DifferneceOnReferenceTables) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_filtered2.tbl", 2);

  std::vector<std::string> column_filter = {"a", "b"};
  auto projection1 = std::make_shared<Projection>(_gt_a, column_filter);
  projection1->execute();

  auto projection2 = std::make_shared<Projection>(_gt_b, column_filter);
  projection2->execute();

  auto difference = std::make_shared<Difference>(projection1, projection2);
  difference->execute();

  EXPECT_TABLE_EQ(difference->get_output(), expected_result);
}

TEST_F(OperatorsDifferenceTest, ThrowWrongColumnNumberException) {
  std::shared_ptr<Table> test_table_c = load_table("src/test/tables/int.tbl", 2);
  StorageManager::get().add_table("difference_test_table_c", std::move(test_table_c));
  auto gt_c = std::make_shared<GetTable>("difference_test_table_c");

  auto difference = std::make_shared<Difference>(_gt_a, gt_c);

  EXPECT_THROW(difference->execute(), std::exception);
}

TEST_F(OperatorsDifferenceTest, ThrowWrongColumnOrderException) {
  std::shared_ptr<Table> test_table_d = load_table("src/test/tables/float_int.tbl", 2);
  StorageManager::get().add_table("difference_test_table_d", std::move(test_table_d));
  auto gt_d = std::make_shared<GetTable>("difference_test_table_d");

  auto difference = std::make_shared<Difference>(_gt_a, gt_d);

  EXPECT_THROW(difference->execute(), std::exception);
}

}  // namespace opossum
