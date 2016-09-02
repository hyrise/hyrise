#include "gtest/gtest.h"

#include "../../lib/storage/table.hpp"

TEST(table, HasOneChunkAfterCreation) {
  opossum::Table t(2);
  EXPECT_EQ(t.chunk_count(), 1u);
}
