#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/maintenance/create_table.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class CreateTableTest : public BaseTest {
  void SetUp() override {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);

    create_table = std::make_shared<CreateTable>("t", column_definitions);
  }

  std::shared_ptr
};

TEST_F(CreateTableTest, NameAndDescription) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Float, true);

  CreateTable create_table{"t", column_definitions};

}

}  // namespace opossum
