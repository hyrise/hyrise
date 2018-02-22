#include <sstream>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "operators/union_positions.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class PhysicalQueryPlanTest : public BaseTest {};

TEST_F(PhysicalQueryPlanTest, Print) {
  StorageManager::get().add_table("int_int_int_100", load_table("src/test/tables/sqlite/int_int_int_100.tbl", 20));

  auto get_table = std::make_shared<GetTable>("int_int_int_100");
  auto table_scan_a = std::make_shared<TableScan>(get_table, ColumnID{0}, PredicateCondition::GreaterThan, 20);
  auto table_scan_b = std::make_shared<TableScan>(get_table, ColumnID{1}, PredicateCondition::GreaterThan, 30);
  auto union_positions = std::make_shared<UnionPositions>(table_scan_a, table_scan_b);

  std::stringstream stream;
  union_positions->print(stream);

  EXPECT_EQ(stream.str(), R"([0] UnionPositions
 \_[1] TableScan (Col #0 > 20)
 |  \_[2] GetTable (int_int_int_100)
 \_[3] TableScan (Col #1 > 30)
    \_Recurring Node --> [2]
)");
}
}  // namespace opossum
