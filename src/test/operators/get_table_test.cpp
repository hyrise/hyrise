#include <memory>

#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {
// The fixture for testing class GetTable.
class OperatorsGetTableTest : public ::testing::Test {
  void SetUp() override {
    test_table = std::make_shared<opossum::Table>(opossum::Table(2));
    opossum::StorageManager::get().add_table("aNiceTestTable", test_table);
  }
  void TearDown() override { opossum::StorageManager::get().drop_table("aNiceTestTable"); }

 public:
  std::shared_ptr<opossum::Table> test_table;
};

TEST_F(OperatorsGetTableTest, GetOutput) {
  auto gt = std::make_shared<opossum::GetTable>("aNiceTestTable");
  gt->execute();

  EXPECT_EQ(gt->get_output(), test_table);
}

TEST_F(OperatorsGetTableTest, ThrowsUnknownTableName) {
  auto gt = std::make_shared<opossum::GetTable>("anUglyTestTable");
  gt->execute();

  EXPECT_THROW(gt->get_output(), std::exception) << "Should throw unkown table name exception";
}

}  // namespace opossum
