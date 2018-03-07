#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {
// The fixture for testing class GetTable.
class OperatorsGetTableTest : public BaseTest {
 protected:
  void SetUp() override {
    _test_table = std::make_shared<Table>(TableColumnDefinitions{}, TableType::Data, 2);
    auto& manager = StorageManager::get();
    manager.add_table("aNiceTestTable", _test_table);
    manager.add_table("tableWithValues", load_table("src/test/tables/int_float2.tbl", 1u));
  }

  std::shared_ptr<Table> _test_table;
};

TEST_F(OperatorsGetTableTest, GetOutput) {
  auto gt = std::make_shared<GetTable>("aNiceTestTable");
  gt->execute();

  EXPECT_EQ(gt->get_output(), _test_table);
}

TEST_F(OperatorsGetTableTest, ThrowsUnknownTableName) {
  auto gt = std::make_shared<GetTable>("anUglyTestTable");

  EXPECT_THROW(gt->execute(), std::exception) << "Should throw unknown table name exception";
}

TEST_F(OperatorsGetTableTest, OperatorName) {
  auto gt = std::make_shared<opossum::GetTable>("aNiceTestTable");

  EXPECT_EQ(gt->name(), "GetTable");
}

TEST_F(OperatorsGetTableTest, ExcludedChunks) {
  auto gt = std::make_shared<opossum::GetTable>("tableWithValues");

  gt->set_excluded_chunk_ids({ChunkID(0), ChunkID(2)});
  gt->execute();

  auto original_table = StorageManager::get().get_table("tableWithValues");
  auto table = gt->get_output();
  EXPECT_EQ(table->chunk_count(), ChunkID(2));
  EXPECT_EQ(table->get_value<int>(ColumnID(0), 0u), original_table->get_value<int>(ColumnID(0), 1u));
  EXPECT_EQ(table->get_value<int>(ColumnID(0), 1u), original_table->get_value<int>(ColumnID(0), 3u));
}

}  // namespace opossum
