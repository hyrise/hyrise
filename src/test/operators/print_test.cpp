#include <memory>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"

namespace opossum {

class OperatorsPrintTest : public BaseTest {};

TEST_F(OperatorsPrintTest, NumInputTables) {
  auto pr = std::make_shared<opossum::Print>(nullptr);

  EXPECT_EQ(pr->num_in_tables(), 1);
}

TEST_F(OperatorsPrintTest, NumOutputTables) {
  auto pr = std::make_shared<opossum::Print>(nullptr);

  EXPECT_EQ(pr->num_out_tables(), 1);
}

TEST_F(OperatorsPrintTest, OperatorName) {
  auto pr = std::make_shared<opossum::Print>(nullptr);

  EXPECT_EQ(pr->name(), "Print");
}

}  // namespace opossum
