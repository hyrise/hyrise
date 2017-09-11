#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/abstract_read_only_operator.hpp"
#include "../../lib/operators/product.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/table_wrapper.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {
class OperatorsProductTest : public BaseTest {
 public:
  std::shared_ptr<opossum::TableWrapper> _table_wrapper_a, _table_wrapper_b;

  virtual void SetUp() {
    _table_wrapper_a = std::make_shared<TableWrapper>(load_table("src/test/tables/int.tbl", 5));

    _table_wrapper_b = std::make_shared<TableWrapper>(load_table("src/test/tables/float.tbl", 2));

    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
  }
};

TEST_F(OperatorsProductTest, ValueColumns) {
  auto product = std::make_shared<Product>(_table_wrapper_a, _table_wrapper_b);
  product->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_product.tbl", 3);
  EXPECT_TABLE_EQ(product->get_output(), expected_result);
}

TEST_F(OperatorsProductTest, ReferenceAndValueColumns) {
  auto table_scan =
      std::make_shared<opossum::TableScan>(_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, 1234);
  table_scan->execute();

  auto product = std::make_shared<Product>(table_scan, _table_wrapper_b);
  product->execute();

  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_filtered_float_product.tbl", 3);
  EXPECT_TABLE_EQ(product->get_output(), expected_result);
}

}  // namespace opossum
