#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_non_modifying_operator.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/product.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {
class OperatorsProductTest : public BaseTest {
 public:
  std::shared_ptr<opossum::GetTable> _gt_a, _gt_b;

  virtual void SetUp() {
    std::shared_ptr<Table> test_table_a = load_table("src/test/tables/int.tbl", 5);
    StorageManager::get().add_table("table_a", std::move(test_table_a));
    _gt_a = std::make_shared<GetTable>("table_a");

    std::shared_ptr<Table> test_table_b = load_table("src/test/tables/float.tbl", 2);
    StorageManager::get().add_table("table_b", std::move(test_table_b));
    _gt_b = std::make_shared<GetTable>("table_b");

    _gt_a->execute();
    _gt_b->execute();
  }
};

TEST_F(OperatorsProductTest, ValueColumns) {
  auto product = std::make_shared<Product>(_gt_a, _gt_b, "left", "right");
  product->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_product.tbl", 3);
  EXPECT_TABLE_EQ(product->get_output(), expected_result);
}

TEST_F(OperatorsProductTest, ReferenceAndValueColumns) {
  auto table_scan = std::make_shared<opossum::TableScan>(_gt_a, "a", ">=", 1234);
  table_scan->execute();

  auto product = std::make_shared<Product>(table_scan, _gt_b, "left", "right");
  product->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_filtered_float_product.tbl", 3);
  EXPECT_TABLE_EQ(product->get_output(), expected_result);
}

}  // namespace opossum
