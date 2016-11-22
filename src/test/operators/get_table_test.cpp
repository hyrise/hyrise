#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {
// The fixture for testing class GetTable.
class OperatorsGetTableTest : public BaseTest {
  virtual void SetUp() {
    test_table = std::make_shared<Table>(Table(2));
    StorageManager::get().add_table("aNiceTestTable", test_table);
  }
  virtual void TearDown() { StorageManager::get().drop_table("aNiceTestTable"); }

 public:
  std::shared_ptr<Table> test_table;
};

TEST_F(OperatorsGetTableTest, GetOutput) {
  auto gt = std::make_shared<GetTable>("aNiceTestTable");
  gt->execute();

  EXPECT_EQ(gt->get_output(), test_table);
}

TEST_F(OperatorsGetTableTest, ThrowsUnknownTableName) {
  auto gt = std::make_shared<GetTable>("anUglyTestTable");
  gt->execute();

  EXPECT_THROW(gt->get_output(), std::exception) << "Should throw unkown table name exception";
}

}  // namespace opossum
