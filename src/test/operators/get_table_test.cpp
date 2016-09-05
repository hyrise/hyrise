#include <memory>

#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {
// The fixture for testing class GetTable.
class OperatorsGetTable : public ::testing::Test {
  virtual void SetUp() {
    test_table = std::make_shared<opossum::Table>(opossum::Table(2));
    opossum::StorageManager::get().add_table("aNiceTestTable", test_table);
  }

 public:
  std::shared_ptr<opossum::Table> test_table;
};

TEST_F(OperatorsGetTable, get_output_returns_correct_table) {
  auto gt = std::make_shared<opossum::GetTable>("aNiceTestTable");
  gt->execute();

  EXPECT_EQ(gt->get_output(), test_table);
}

TEST_F(OperatorsGetTable, get_output_throwns_on_unknown_table_name) {
  auto gt = std::make_shared<opossum::GetTable>("anUglyTestTable");
  gt->execute();

  EXPECT_THROW(gt->get_output(), std::exception) << "Should throw unkown table name exception";
}

}  // namespace opossum
