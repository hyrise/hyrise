#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/maintenance/show_tables.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

#include "utils/assert.hpp"

namespace opossum {

class ShowTablesTest : public BaseTest {};

TEST_F(ShowTablesTest, OperatorName) {
  auto st = std::make_shared<ShowTables>();

  EXPECT_EQ(st->name(), "ShowTables");
}

TEST_F(ShowTablesTest, CanBeCopied) {
  auto st = std::make_shared<ShowTables>();

  auto copy = st->deep_copy();
  ASSERT_NE(nullptr, std::dynamic_pointer_cast<ShowTables>(copy));
  ASSERT_NE(st, copy) << "Copy returned the same object";
}

TEST_F(ShowTablesTest, CanShowTables) {
  auto& sm = StorageManager::get();

  sm.add_table("first_table", std::make_shared<Table>(TableCxlumnDefinitions{}, TableType::Data));
  sm.add_table("second_table", std::make_shared<Table>(TableCxlumnDefinitions{}, TableType::Data));

  auto st = std::make_shared<ShowTables>();
  st->execute();

  auto out = st->get_output();
  EXPECT_EQ(out->row_count(), 2u) << "ShowTables returned wrong number of tables";
  EXPECT_EQ(out->cxlumn_count(), 1u) << "ShowTables returned wrong number of cxlumns";

  auto segment =
      std::static_pointer_cast<const ValueSegment<std::string>>(out->get_chunk(ChunkID{0})->get_segment(CxlumnID{0}));
  EXPECT_EQ(segment->values()[0], "first_table");
  EXPECT_EQ(segment->values()[1], "second_table");
}

TEST_F(ShowTablesTest, NoTables) {
  auto st = std::make_shared<ShowTables>();
  st->execute();

  auto out = st->get_output();
  EXPECT_EQ(out->row_count(), 0u) << "ShowTables returned wrong number of tables";
  EXPECT_EQ(out->cxlumn_count(), 1u) << "ShowTables returned wrong number of cxlumns";
}

}  // namespace opossum
