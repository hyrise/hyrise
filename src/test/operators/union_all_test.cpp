#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/union_all.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"

namespace opossum {
class OperatorsUnionAllTest : public BaseTest {
 protected:
  virtual void SetUp() {
    std::shared_ptr<Table> test_table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("union_test_table_a", std::move(test_table_a));
    _gt_a = std::make_shared<GetTable>("union_test_table_a");

    std::shared_ptr<Table> test_table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("union_test_table_b", std::move(test_table_b));
    _gt_b = std::make_shared<GetTable>("union_test_table_b");

    _gt_a->execute();
    _gt_b->execute();
  }

  std::shared_ptr<GetTable> _gt_a;
  std::shared_ptr<GetTable> _gt_b;
};

TEST_F(OperatorsUnionAllTest, UnionOfValueTables) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_union.tbl", 2);

  auto union_all = std::make_shared<UnionAll>(_gt_a, _gt_b);
  union_all->execute();

  EXPECT_TABLE_EQ(union_all->get_output(), expected_result);
}

TEST_F(OperatorsUnionAllTest, UnionOfValueReferenceTables) {
  std::shared_ptr<Table> expected_result = load_table("src/test/tables/int_float_union.tbl", 2);

  std::vector<std::string> column_filter = {"a", "b"};
  auto projection = std::make_shared<Projection>(_gt_a, column_filter);
  projection->execute();

  auto union_all = std::make_shared<UnionAll>(projection, _gt_b);
  union_all->execute();

  EXPECT_TABLE_EQ(union_all->get_output(), expected_result);
}

TEST_F(OperatorsUnionAllTest, ThrowWrongColumnNumberException) {
  if (!IS_DEBUG) return;
  std::shared_ptr<Table> test_table_c = load_table("src/test/tables/int.tbl", 2);
  StorageManager::get().add_table("union_test_table_c", std::move(test_table_c));
  auto gt_c = std::make_shared<GetTable>("union_test_table_c");
  gt_c->execute();

  auto union_all = std::make_shared<UnionAll>(_gt_a, gt_c);

  EXPECT_THROW(union_all->execute(), std::exception);
}

TEST_F(OperatorsUnionAllTest, ThrowWrongColumnOrderException) {
  if (!IS_DEBUG) return;
  std::shared_ptr<Table> test_table_d = load_table("src/test/tables/float_int.tbl", 2);
  StorageManager::get().add_table("union_all_test_table_d", std::move(test_table_d));
  auto gt_d = std::make_shared<GetTable>("union_all_test_table_d");
  gt_d->execute();

  auto union_all = std::make_shared<UnionAll>(_gt_a, gt_d);

  EXPECT_THROW(union_all->execute(), std::exception);
}

}  // namespace opossum
