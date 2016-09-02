#include "gtest/gtest.h"

#include "../../lib/storage/table.hpp"

TEST(storage_table, has_one_chunk_after_creation) {
  opossum::Table t(2);
  EXPECT_EQ(t.chunk_count(), 1u);
}
