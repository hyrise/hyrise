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
    _test_table = std::make_shared<Table>(2);
    StorageManager::get().add_table("aNiceTestTable", _test_table);
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

}  // namespace opossum
